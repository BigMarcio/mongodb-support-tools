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
    maxUniqueDocs: 200000,
    appliers: 128,
    spreadThreshold: 5,
    minDocCps: 2,
    minTotalCps: 10,
    topDocCountForProxy: 5,
    outputFormat: "json",
    outputFile: "hot-doc-spread-check.json"
  };

  const INPUT = globalThis.MONITOR_ARGS || {};
  const CFG = Object.assign({}, DEFAULTS, INPUT);
  if (INPUT.outputMarkdown === true) {
    CFG.outputFormat = "markdown";
  }
  if (INPUT.spreadDisparityThreshold !== undefined) {
    CFG.spreadThreshold = INPUT.spreadDisparityThreshold;
  }

  function validateNumber(name, value, allowZero) {
    if (!Number.isFinite(value) || (!allowZero && value <= 0) || (allowZero && value < 0)) {
      throw new Error(`Invalid ${name}: ${value}`);
    }
  }

  validateNumber("lookbackMs", CFG.lookbackMs, false);
  validateNumber("runMs", CFG.runMs, false);
  validateNumber("idleMs", CFG.idleMs, true);
  validateNumber("maxUniqueDocs", CFG.maxUniqueDocs, false);
  validateNumber("appliers", CFG.appliers, false);
  validateNumber("spreadDisparityThreshold", CFG.spreadThreshold, false);
  validateNumber("minDocCps", CFG.minDocCps, false);
  validateNumber("minTotalCps", CFG.minTotalCps, true);
  validateNumber("topDocCountForProxy", CFG.topDocCountForProxy, false);

  if (!Number.isInteger(CFG.maxUniqueDocs) || CFG.maxUniqueDocs < 1) {
    throw new Error(`Invalid maxUniqueDocs: ${CFG.maxUniqueDocs}. Expected an integer greater than or equal to 1.`);
  }

  if (CFG.appliers < 2) {
    throw new Error(`Invalid appliers: ${CFG.appliers}. Expected a value of at least 2.`);
  }

  if (!Number.isInteger(CFG.topDocCountForProxy) || CFG.topDocCountForProxy < 1 || CFG.topDocCountForProxy > 100) {
    throw new Error(`Invalid topDocCountForProxy: ${CFG.topDocCountForProxy}. Expected an integer between 1 and 100.`);
  }

  if (!Array.isArray(CFG.namespaces)) {
    throw new Error('namespaces must be an array, e.g. ["db1.coll1", "db2.coll2"]');
  }

  function formatDurationMs(ms) {
    if (ms % 60000 === 0) return `${ms / 60000}m`;
    if (ms % 1000 === 0) return `${ms / 1000}s`;
    return `${ms}ms`;
  }

  const outputFormat = String(CFG.outputFormat || "json").toLowerCase();
  if (!["json", "markdown", "both"].includes(outputFormat)) {
    throw new Error(`Invalid outputFormat: ${CFG.outputFormat}. Expected one of: json, markdown, both.`);
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

  if (typeof cs.disableBlockWarnings === "function") {
    cs.disableBlockWarnings();
  }

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
  let uniqueDocsTracked = 0;
  let lastEventAt = Date.now();
  let lastClusterTime = null;
  let stopReason = "unknown";

  const spreadDisparityThreshold = CFG.spreadThreshold;
  const sqrtAppliersMinusOne = Math.sqrt(CFG.appliers - 1);
  const minShareRequired = spreadDisparityThreshold / sqrtAppliersMinusOne;
  const lookbackCurrentLabel = formatDurationMs(CFG.lookbackMs);
  const lookbackDefaultLabel = formatDurationMs(DEFAULTS.lookbackMs);

  print("");
  if (watchAllNamespaces) {
    print(`Reading change stream for ALL namespaces from ${new Date(startSec * 1000).toISOString()} (lookback interval: current=${lookbackCurrentLabel}, default=${lookbackDefaultLabel}) ...`);
  } else {
    print(`Reading change stream for namespaces [${watchedNamespaces.join(", ")}] from ${new Date(startSec * 1000).toISOString()} (lookback interval: current=${lookbackCurrentLabel}, default=${lookbackDefaultLabel}) ...`);
  }
  print("");

  while (true) {
    if (Date.now() > deadline) {
      stopReason = "run-ms-exceeded";
      break;
    }

    if (CFG.idleMs > 0 && Date.now() - lastEventAt > CFG.idleMs && matchedEventsSeen > 0) {
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
      if (uniqueDocsTracked >= CFG.maxUniqueDocs) {
        stopReason = "max-unique-docs-reached";
        print(`Stopping early after reaching maxUniqueDocs=${CFG.maxUniqueDocs}. Increase maxUniqueDocs to analyze larger cardinality workloads.`);
        break;
      }

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
      uniqueDocsTracked++;
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
          const passesSpreadGate = spreadDisparityEstimate >= spreadDisparityThreshold;
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

  const topDocCountForProxy = CFG.topDocCountForProxy;
  const topDocuments = allDocs.slice(0, topDocCountForProxy);
  const topCombinedOps = topDocuments.reduce((sum, d) => sum + d.opCount, 0);
  const topCombinedShare = qualifiedEventsSeen === 0 ? 0 : topCombinedOps / qualifiedEventsSeen;
  const topCombinedSingleDocSpreadEquivalent = topCombinedShare * sqrtAppliersMinusOne;

  const output = {
    generatedAt: new Date().toISOString(),
    namespacesRequested: watchAllNamespaces ? "ALL" : watchedNamespaces,
    lookbackMs: CFG.lookbackMs,
    appliers: CFG.appliers,
    spreadDisparityThreshold,
    spreadThreshold: spreadDisparityThreshold,
    minShareRequired,
    minDocCps: CFG.minDocCps,
    minTotalCps: CFG.minTotalCps,
    maxUniqueDocs: CFG.maxUniqueDocs,
    matchedEventsSeen,
    qualifiedEventsSeen,
    uniqueDocsTracked,
    totalChangesPerSec,
    stopReason,
    cappedByMaxUniqueDocs: stopReason === "max-unique-docs-reached",
    lastClusterTime,
    topCombined: {
      requestedDocCount: topDocCountForProxy,
      effectiveDocCount: topDocuments.length,
      opCount: topCombinedOps,
      share: topCombinedShare,
      singleDocSpreadEquivalent: topCombinedSingleDocSpreadEquivalent,
      documents: topDocuments
    },
    hotDocuments
  };

  function toFixedNumber(v, digits) {
    return Number(v).toFixed(digits);
  }

  function toPct(v, digits) {
    return `${(v * 100).toFixed(digits)}%`;
  }

  function mdEscape(v) {
    return String(v).replace(/\|/g, "\\|");
  }

  function buildMarkdownTableRows(docs) {
    return docs
      .map((d, i) => {
        return `| ${i + 1} | ${d.opCount} | ${d.insert} | ${d.delete} | ${d.update} | ${toPct(d.eventShare, 2)} | ${toFixedNumber(d.spreadDisparityEstimate, 3)} | ${toFixedNumber(d.docChangesPerSec, 3)} | ${toFixedNumber(d.totalChangesPerSec, 3)} | ${mdEscape(d.ns)} | ${mdEscape(bsonString(d.docId))} |`;
      })
      .join("\n");
  }

  function buildMarkdownReport() {
    const scopeValue = watchAllNamespaces ? "ALL" : watchedNamespaces.join(", ");
    const topTableRows = buildMarkdownTableRows(topDocuments);
    const hotTableRows = buildMarkdownTableRows(hotDocuments);
    const topTable = topTableRows || "| - | - | - | - | - | - | - | - | - | - | - |";
    const hotTable = hotTableRows || "| - | - | - | - | - | - | - | - | - | - | - |";

    return [
      "# Hot Doc Spread Check Report",
      "",
      `Generated at: ${output.generatedAt}`,
      `Namespaces requested: ${scopeValue}`,
      `Stop reason: ${stopReason}`,
      "",
      "## Summary",
      "",
      `- Appliers: ${CFG.appliers}`,
      `- SpreadDisparityThreshold: ${spreadDisparityThreshold}`,
      `- RequiredShare(single-doc): ${toPct(minShareRequired, 2)}`,
      `- MinDocCps: ${CFG.minDocCps}  MinTotalCps: ${CFG.minTotalCps} (Cps means changes per second)`,
      `- Unique docs tracked: ${uniqueDocsTracked} / ${CFG.maxUniqueDocs}`,
      `- Qualified changes in window: ${qualifiedEventsSeen}`,
      `- Total changes/sec: ${toFixedNumber(totalChangesPerSec, 3)}`,
      `- Top-${topDocCountForProxy} combined ops: ${topCombinedOps}`,
      `- Top-${topDocCountForProxy} combined share: ${toPct(topCombinedShare, 2)}`,
      `- Top-${topDocCountForProxy} single-doc spread equivalent: ${toFixedNumber(topCombinedSingleDocSpreadEquivalent, 3)}`,
      "",
      `## Top-${topDocCountForProxy} Documents (By Op Count)`,
      "",
      "| rank | opCount | insert | delete | update | share% | spreadDisp | docCps | totalCps | namespace | _id |",
      "| ---: | ------: | -----: | -----: | -----: | -----: | ---------: | -----: | -------: | :-------- | :-- |",
      topTable,
      "",
      "## Hot Documents",
      "",
      "| rank | opCount | insert | delete | update | share% | spreadDisp | docCps | totalCps | namespace | _id |",
      "| ---: | ------: | -----: | -----: | -----: | -----: | ---------: | -----: | -------: | :-------- | :-- |",
      hotTable,
      ""
    ].join("\n");
  }

  function deriveOutputPath(format) {
    if (format === "json") {
      return CFG.outputFile;
    }
    if (format === "markdown") {
      if (CFG.outputFile === DEFAULTS.outputFile) {
        return "hot-doc-spread-check.md";
      }
      return CFG.outputFile;
    }
    return null;
  }

  if (outputFormat === "json" || outputFormat === "both") {
    const jsonPath = outputFormat === "json" ? deriveOutputPath("json") : DEFAULTS.outputFile;
    fs.writeFileSync(
      jsonPath,
      EJSON.stringify(output, null, 2, { relaxed: false })
    );
    print(`Wrote JSON results to ${jsonPath}`);
  }

  if (outputFormat === "markdown" || outputFormat === "both") {
    const markdownPath = outputFormat === "markdown" ? deriveOutputPath("markdown") : "hot-doc-spread-check.md";
    fs.writeFileSync(markdownPath, buildMarkdownReport());
    print(`Wrote Markdown results to ${markdownPath}`);
  }

  print("");
  print("=".repeat(112));
  print(`TOP-${topDocCountForProxy} COMBINED SHARE`);
  print("=".repeat(112));
  print(`Appliers: ${CFG.appliers}  SpreadDisparityThreshold: ${spreadDisparityThreshold}  RequiredShare(single-doc): ${(minShareRequired * 100).toFixed(2)}%`);
  print(`MinDocCps: ${CFG.minDocCps}  MinTotalCps: ${CFG.minTotalCps} (Cps means changes per second)`);
  print(`Unique docs tracked: ${uniqueDocsTracked} / ${CFG.maxUniqueDocs}`);
  print(`Qualified changes in window: ${qualifiedEventsSeen}  Total changes/sec: ${totalChangesPerSec.toFixed(3)}`);
  print(`Top-${topDocCountForProxy} combined ops   : ${topCombinedOps}`);
  print(`Top-${topDocCountForProxy} combined share : ${(topCombinedShare * 100).toFixed(2)}%`);
  print(`Top-${topDocCountForProxy} single-doc spread equivalent : ${topCombinedSingleDocSpreadEquivalent.toFixed(3)}`);

  print("");
  print("=".repeat(112));
  print(`TOP-${topDocCountForProxy} DOCUMENTS (BY OP COUNT)`);
  print("=".repeat(112));
  const col = {
    rank: 4,
    opCount: 7,
    insert: 6,
    delete: 6,
    update: 6,
    sharePct: 7,
    spreadDisp: 10,
    docCps: 7,
    totalCps: 8,
    namespace: 22
  };

  function makeTableHeader() {
    return (
      `${"rank".padStart(col.rank)}  ` +
      `${"opCount".padStart(col.opCount)}  ` +
      `${"insert".padStart(col.insert)}  ` +
      `${"delete".padStart(col.delete)}  ` +
      `${"update".padStart(col.update)}  ` +
      `${"share%".padStart(col.sharePct)}  ` +
      `${"spreadDisp".padStart(col.spreadDisp)}  ` +
      `${"docCps".padStart(col.docCps)}  ` +
      `${"totalCps".padStart(col.totalCps)}  ` +
      `${"namespace".padEnd(col.namespace)}  _id`
    );
  }

  function makeTableRow(d, rank) {
    return (
      `${String(rank).padStart(col.rank)}  ` +
      `${String(d.opCount).padStart(col.opCount)}  ` +
      `${String(d.insert).padStart(col.insert)}  ` +
      `${String(d.delete).padStart(col.delete)}  ` +
      `${String(d.update).padStart(col.update)}  ` +
      `${(d.eventShare * 100).toFixed(2).padStart(col.sharePct)}  ` +
      `${d.spreadDisparityEstimate.toFixed(3).padStart(col.spreadDisp)}  ` +
      `${d.docChangesPerSec.toFixed(3).padStart(col.docCps)}  ` +
      `${d.totalChangesPerSec.toFixed(3).padStart(col.totalCps)}  ` +
      `${d.ns.padEnd(col.namespace)}  ` +
      `${bsonString(d.docId)}`
    );
  }

  print(makeTableHeader());

  topDocuments.forEach((d, i) => {
    print(makeTableRow(d, i + 1));
  });

  if (topDocuments.length === 0) {
    print("No documents found in this sample.");
  }

  print("");

  print("");
  print("=".repeat(112));
  print("HOT DOCUMENTS");
  print("=".repeat(112));
  print(makeTableHeader());

  hotDocuments.forEach((d, i) => {
    print(makeTableRow(d, i + 1));
  });

  if (hotDocuments.length === 0) {
    print("No documents passed all gates in this sample.");
  }

  print("");
})();
