// hot-doc-spread-check.js
(function () {
  const fs = require("fs");

  function bsonString(v) {
    return EJSON.stringify(v, null, 0, { relaxed: false });
  }

  const DEFAULTS = {
    namespaces: [],
    lookbackMs: 5 * 60 * 1000,
    runMs: 5 * 60 * 1000,
    idleMs: 2000,
    appliers: 128,
    spreadThreshold: 5,
    minDocCps: 2,
    minTotalCps: 10,
    outputFile: "hot-doc-spread-check.json"
  };

  const INPUT = globalThis.MONITOR_ARGS || {};
  const CFG = Object.assign({}, DEFAULTS, INPUT);

  function validateNumber(name, value, allowZero) {
    if (!Number.isFinite(value) || (!allowZero && value <= 0) || (allowZero && value < 0)) {
      throw new Error(`Invalid ${name}: ${value}`);
    }
  }

  validateNumber("lookbackMs", CFG.lookbackMs, true);
  validateNumber("runMs", CFG.runMs, false);
  validateNumber("idleMs", CFG.idleMs, true);
  validateNumber("appliers", CFG.appliers, false);
  validateNumber("spreadThreshold", CFG.spreadThreshold, false);
  validateNumber("minDocCps", CFG.minDocCps, false);
  validateNumber("minTotalCps", CFG.minTotalCps, true);

  if (CFG.appliers < 2) {
    throw new Error(`Invalid appliers: ${CFG.appliers}. Expected a value of at least 2.`);
  }

  if (!Array.isArray(CFG.namespaces)) {
    throw new Error('namespaces must be an array, e.g. ["db1.coll1", "db2.coll2"]');
  }

  function parseNamespace(ns) {
    const dot = ns.indexOf(".");
    if (dot <= 0 || dot === ns.length - 1) {
      throw new Error(`Invalid namespace "${ns}". Expected format: database.collection`);
    }
    return { db: ns.slice(0, dot), coll: ns.slice(dot + 1) };
  }

  const watchedNamespaces = CFG.namespaces.map(String);
  const watchAllNamespaces = watchedNamespaces.length === 0;
  const namespaceFilters = watchedNamespaces.map(parseNamespace);

  const startSec = Math.floor((Date.now() - CFG.lookbackMs) / 1000);
  const startAt = Timestamp(startSec, 0);
  const stopAtMs = Date.now();
  const deadline = Date.now() + CFG.runMs;

  const matchStage = {
    operationType: { $in: ["insert", "update", "delete", "replace"] }
  };

  if (!watchAllNamespaces) {
    matchStage.$or = namespaceFilters.map((x) => ({
      "ns.db": x.db,
      "ns.coll": x.coll
    }));
  } else {
    matchStage.$and = [
      {
        "ns.db": {
          $nin: ["admin", "config", "local"]
        }
      },
      {
        "ns.coll": {
          $not: /^system\./
        }
      }
    ];
  }

  const cs = db.getMongo().watch(
    [
      { $match: matchStage },
      {
        $project: {
          _id: 1,
          operationType: 1,
          clusterTime: 1,
          "ns.db": 1,
          "ns.coll": 1,
          "documentKey._id": 1
        }
      }
    ],
    {
      startAtOperationTime: startAt,
      maxAwaitTimeMS: 250
    }
  );

  cs.disableBlockWarnings();

  const perNs = Object.create(null);
  const nsOrder = [];
  const opMap = {
    insert: "insert",
    update: "update",
    replace: "update",
    delete: "delete"
  };

  let matchedEventsSeen = 0;
  let qualifiedEventsSeen = 0;
  let lastEventAt = Date.now();
  let lastClusterTime = null;
  let stopReason = "unknown";

  const sqrtAppliersMinusOne = Math.sqrt(CFG.appliers - 1);
  const minShareRequired = CFG.spreadThreshold / sqrtAppliersMinusOne;

  if (watchAllNamespaces) {
    print(`Reading change stream for ALL namespaces from ${new Date(startSec * 1000).toISOString()} ...`);
  } else {
    print(`Reading change stream for namespaces [${watchedNamespaces.join(", ")}] from ${new Date(startSec * 1000).toISOString()} ...`);
  }

  while (true) {
    if (Date.now() > deadline) {
      stopReason = "run-ms-exceeded";
      break;
    }

    if (Date.now() - lastEventAt > CFG.idleMs && matchedEventsSeen > 0) {
      stopReason = "idle-timeout";
      break;
    }

    const evt = cs.tryNext();
    if (evt === null) {
      sleep(50);
      continue;
    }

    matchedEventsSeen++;
    lastEventAt = Date.now();
    lastClusterTime = evt.clusterTime;

    if (evt.clusterTime && evt.clusterTime.t * 1000 >= stopAtMs) {
      stopReason = "caught-up";
    }

    if (!evt.ns || !evt.ns.db || !evt.ns.coll) {
      if (stopReason === "caught-up") break;
      continue;
    }

    if (evt.ns.db === "admin" || evt.ns.db === "config" || evt.ns.db === "local" || evt.ns.coll.startsWith("system.")) {
      if (stopReason === "caught-up") break;
      continue;
    }

    const docId = evt.documentKey && evt.documentKey._id;
    if (docId === undefined || docId === null) {
      if (stopReason === "caught-up") break;
      continue;
    }

    const opName = opMap[evt.operationType];
    if (!opName) {
      if (stopReason === "caught-up") break;
      continue;
    }

    const ns = `${evt.ns.db}.${evt.ns.coll}`;
    const key = bsonString(docId);

    let nsBucket = perNs[ns];
    if (!nsBucket) {
      nsBucket = { rows: Object.create(null), keys: [] };
      perNs[ns] = nsBucket;
      nsOrder.push(ns);
    }

    let row = nsBucket.rows[key];
    if (!row) {
      row = {
        ns,
        docId,
        opCount: 0,
        insert: 0,
        update: 0,
        delete: 0
      };
      nsBucket.rows[key] = row;
      nsBucket.keys.push(key);
    }

    row.opCount++;
    row[opName]++;
    qualifiedEventsSeen++;

    if (stopReason === "caught-up") break;
  }

  cs.close();

  print(`Stream closed. Reason: ${stopReason}. Matched events: ${matchedEventsSeen}. Qualified doc events: ${qualifiedEventsSeen}.`);

  const windowSeconds = CFG.lookbackMs / 1000;
  const totalChangesPerSec = windowSeconds > 0 ? qualifiedEventsSeen / windowSeconds : 0;

  const allDocsRaw = [];
  for (const ns of nsOrder) {
    const bucket = perNs[ns];
    for (const key of bucket.keys) {
      allDocsRaw.push(bucket.rows[key]);
    }
  }

  const allDocs = qualifiedEventsSeen === 0
    ? []
    : allDocsRaw
        .map((d) => {
          const eventShare = d.opCount / qualifiedEventsSeen;
          const spreadDisparityEstimate = eventShare * sqrtAppliersMinusOne;
          const docChangesPerSec = windowSeconds > 0 ? d.opCount / windowSeconds : 0;
          const passesSpreadGate = spreadDisparityEstimate >= CFG.spreadThreshold;
          const passesDocCpsGate = docChangesPerSec >= CFG.minDocCps;
          const passesTotalCpsGate = totalChangesPerSec >= CFG.minTotalCps;
          const qualifies = passesSpreadGate && passesDocCpsGate && passesTotalCpsGate;

          return {
            ns: d.ns,
            docId: d.docId,
            docChanges: d.opCount,
            opCount: d.opCount,
            insert: d.insert,
            update: d.update,
            delete: d.delete,
            eventShare,
            spreadDisparityEstimate,
            docChangesPerSec,
            totalChangesPerSec,
            passesSpreadGate,
            passesDocCpsGate,
            passesTotalCpsGate,
            qualifies
          };
        })
        .sort((a, b) => {
          if (b.opCount !== a.opCount) return b.opCount - a.opCount;
          return a.ns.localeCompare(b.ns);
        });

  const hotDocuments = allDocs.filter((d) => d.qualifies);

  const top5Documents = allDocs.slice(0, 5);
  const top5CombinedOps = top5Documents.reduce((sum, d) => sum + d.opCount, 0);
  const top5CombinedShare = qualifiedEventsSeen === 0 ? 0 : top5CombinedOps / qualifiedEventsSeen;
  const top5CombinedSingleDocSpreadEquivalent = top5CombinedShare * sqrtAppliersMinusOne;

  const output = {
    generatedAt: new Date().toISOString(),
    namespacesRequested: watchAllNamespaces ? "ALL" : watchedNamespaces,
    lookbackMs: CFG.lookbackMs,
    appliers: CFG.appliers,
    spreadThreshold: CFG.spreadThreshold,
    minShareRequired,
    minDocCps: CFG.minDocCps,
    minTotalCps: CFG.minTotalCps,
    matchedEventsSeen,
    qualifiedEventsSeen,
    totalChangesPerSec,
    stopReason,
    lastClusterTime,
    top5Combined: {
      opCount: top5CombinedOps,
      share: top5CombinedShare,
      singleDocSpreadEquivalent: top5CombinedSingleDocSpreadEquivalent,
      documents: top5Documents
    },
    hotDocuments
  };

  fs.writeFileSync(
    "./" + CFG.outputFile,
    EJSON.stringify(output, null, 2, { relaxed: false })
  );
  print(`Wrote results to ${CFG.outputFile}`);

  print("");
  print("=".repeat(112));
  print("TOP-5 COMBINED SHARE");
  print("=".repeat(112));
  print(`Appliers: ${CFG.appliers}  SpreadThreshold: ${CFG.spreadThreshold}  RequiredShare(single-doc): ${(minShareRequired * 100).toFixed(2)}%`);
  print(`MinDocCps: ${CFG.minDocCps}  MinTotalCps: ${CFG.minTotalCps}`);
  print(`Qualified changes in window: ${qualifiedEventsSeen}  Total changes/sec: ${totalChangesPerSec.toFixed(3)}`);
  print(`Top-5 combined ops   : ${top5CombinedOps}`);
  print(`Top-5 combined share : ${(top5CombinedShare * 100).toFixed(2)}%`);
  print(`Top-5 single-doc spread equivalent : ${top5CombinedSingleDocSpreadEquivalent.toFixed(3)}`);

  print("");
  print("=".repeat(112));
  print("HOT DOCUMENTS");
  print("=".repeat(112));
  print("rank  opCount   insert  delete  update   share%   spreadDisp   docCps   totalCps   namespace               _id");

  hotDocuments.forEach((d, i) => {
    print(
      `${String(i + 1).padStart(4)}  ` +
      `${String(d.opCount).padStart(7)}  ` +
      `${String(d.insert).padStart(6)}  ` +
      `${String(d.delete).padStart(6)}  ` +
      `${String(d.update).padStart(6)}  ` +
      `${(d.eventShare * 100).toFixed(2).padStart(7)}  ` +
      `${d.spreadDisparityEstimate.toFixed(3).padStart(10)}  ` +
      `${d.docChangesPerSec.toFixed(3).padStart(7)}  ` +
      `${d.totalChangesPerSec.toFixed(3).padStart(8)}   ` +
      `${d.ns.padEnd(22)}  ` +
      `${bsonString(d.docId)}`
    );
  });

  if (hotDocuments.length === 0) {
    print("No documents passed all gates in this sample.");
  }
})();
