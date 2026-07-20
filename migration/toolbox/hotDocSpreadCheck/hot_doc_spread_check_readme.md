# Hot Doc Spread Check

## Purpose

`hot-doc-spread-check.js` is a mongosh script that scans recent change activity and identifies documents that are hot enough to plausibly explain high applier skew.

In this script, a document is considered a hot-doc candidate only if it passes all three gates:

* spread-disparity gate
* per-document changes-per-second gate
* total-window changes-per-second gate

The spread-disparity part is based on the internal hot-doc detection proposal, which models the single-hot-document case as:

$$
\text{SpreadDisparity} \approx f \cdot \sqrt{N-1}
$$

where:

* \(N\) = number of appliers
* \(f\) = fraction of all events in the observed window that belong to one document

For a detection threshold of 5, the corresponding per-document share threshold is:

$$
f \ge \frac{5}{\sqrt{N-1}}
$$

For \(N=128\), that is about **44.4%** of all events in the window.

## What the script counts

The script watches change-stream events and counts only these operation types:

* `insert`
* `update`
* `replace` (counted as `update`)
* `delete`

For each document, it tracks:

* total operation count
* insert count
* update count
* delete count

## Current default settings

These are the current defaults used by the script in this README:

```js
{
  namespaces: [],
  lookbackMs: 5 * 60 * 1000,
  runMs: 5 * 60 * 1000,
  idleMs: 2000,
  appliers: 128,
  spreadThreshold: 5,
  minDocCps: 2,
  minTotalCps: 10,
  outputFile: "hot-doc-spread-check.json"
}
```

Meaning:

* `namespaces: []`
  * watch all namespaces
* `lookbackMs: 5 minutes`
  * start reading from 5 minutes in the past
* `runMs: 5 minutes`
  * hard maximum runtime
* `idleMs: 2000`
  * stop after 2 seconds of inactivity once events have started
* `appliers: 128`
  * used in the spread-disparity math
* `spreadThreshold: 5`
  * the spread-disparity activation threshold used by the proposal
* `minDocCps: 2`
  * a document must average at least 2 changes/sec in the window
* `minTotalCps: 10`
  * the whole window must average at least 10 changes/sec

## How the script works

### 1. It opens a deployment-level change stream

The script uses a deployment-level watch so it can observe one namespace, several namespaces, or all namespaces, depending on `namespaces`.

If `namespaces` is empty, it watches everything except internal databases (`admin`, `config`, `local`) and system collections.

If `namespaces` is set, each entry must be in:

```text
database.collection
```

format.

### 2. It replays a recent time window

The script starts from:

```text
now - lookbackMs
```

and replays matching change events from that point forward.

This means the script is primarily a **recent-window sampler**, not a forever-running live monitor.

### 3. It filters to document-level write activity

Only `insert`, `update`, `replace`, and `delete` are counted.

Events without a document `_id` are ignored.

System collections are ignored.

### 4. It aggregates counts per document

For every namespace and `_id`, the script builds a row like:

```js
{
  ns,
  docId,
  opCount,
  insert,
  update,
  delete
}
```

### 5. It computes window-wide totals

After the stream closes, the script computes:

* `qualifiedEventsSeen`
  * all counted document-level write events in the window
* `totalChangesPerSec`
  * `qualifiedEventsSeen / windowSeconds`

### 6. It computes the per-document hotness metrics

For each document, it computes:

```text
eventShare = docChanges / allChangesInWindow
spreadDisparityEstimate = eventShare * sqrt(appliers - 1)
docChangesPerSec = docChanges / windowSeconds
```

The spread formula and the threshold logic come directly from the internal proposal’s single-hot-document derivation.

### 7. It applies three gates

A document is returned in `hotDocuments` only if all three are true:

```text
spreadDisparityEstimate >= spreadThreshold
docChangesPerSec >= minDocCps
totalChangesPerSec >= minTotalCps
```

## Why the script uses the two extra CPS gates

The proposal explicitly notes that a pure spread-disparity check should also be gated by an events-per-second style threshold to avoid false positives during slow periods.

The two extra checks serve different purposes:

### `docChangesPerSec >= minDocCps`

This prevents a document from looking “hot” only because the sample window is tiny or quiet.

Example: if one doc gets 2 out of 3 events, its share is large, but that does not necessarily make it operationally important.

### `totalChangesPerSec >= minTotalCps`

This prevents the whole sample window from being treated as meaningful when overall traffic is too low.

This is effectively a sample-quality / throughput floor.

## What the script is good at detecting

This script is best understood as a **single-document detector**.

It works well when:

* one document dominates the window
* or multiple documents each independently dominate enough to cross the single-doc threshold

For example, with \(N=128\) and threshold 5, a document needs about **44.4%** of all window events to pass individually.

That also means the maximum number of documents that can each pass individually is limited by total share. For \(N=128\), at most **2** documents can each be above the threshold at the same time, because 3 documents at ~44.4% each would require more than 100% total traffic.

## Important limitation

The spread-disparity math used here is derived under these assumptions:

* non-hot documents are spread evenly across appliers
* exactly 1 hot document exists
* the hot document has a stable rate over the measurement window

So this script may under-detect **multi-hot** cases where:

* no single document reaches the threshold
* but 2 or 3 documents together create the skew

That is a limitation of the underlying approximation, not a bug in the script.

## Stop conditions

The script stops when any of these happens:

* `caught-up`
  * it has replayed forward to the “now” that existed when the stream was opened
* `idle-timeout`
  * no matching events arrive for `idleMs` after activity has started
* `run-ms-exceeded`
  * hard max runtime reached

## Output

The script writes a JSON file and also prints a console summary.

### JSON output

Top-level fields include:

* `generatedAt`
* `namespacesRequested`
* `lookbackMs`
* `appliers`
* `spreadThreshold`
* `minShareRequired`
* `minDocCps`
* `minTotalCps`
* `matchedEventsSeen`
* `qualifiedEventsSeen`
* `totalChangesPerSec`
* `stopReason`
* `lastClusterTime`
* `hotDocuments`
* `top5Combined.singleDocSpreadEquivalent`

Each `hotDocuments` entry includes:

```js
{
  ns,
  docId,
  docChanges,
  opCount,
  insert,
  update,
  delete,
  eventShare,
  spreadDisparityEstimate,
  docChangesPerSec,
  totalChangesPerSec,
  passesSpreadGate,
  passesDocCpsGate,
  passesTotalCpsGate,
  qualifies
}
```

### Console output

The console output prints:

* overall window stats
* threshold values
* hot documents that passed all gates
* the top-5 single-doc spread equivalent heuristic

## How to run it

### Run against all namespaces

```bash
mongosh "mongodb://user:password@host/admin" \
  --file hot-doc-spread-check.js
```

### Run against selected namespaces

```bash
mongosh "mongodb://user:password@host/admin" \
  --eval 'globalThis.MONITOR_ARGS = { namespaces: ["sdtest5.events"] }' \
  --file hot-doc-spread-check.js
```

### Override thresholds

```bash
mongosh "mongodb://user:password@host/admin" \
  --eval 'globalThis.MONITOR_ARGS = {
    namespaces: ["sdtest5.events"],
    appliers: 128,
    spreadThreshold: 5,
    minDocCps: 2,
    minTotalCps: 10,
    lookbackMs: 300000
  }' \
  --file hot-doc-spread-check.js
```

## How to read the results

### If `hotDocuments` is empty

Possible reasons:

* no document crossed the spread threshold
* a document crossed the spread threshold but failed `minDocCps`
* the whole sample window failed `minTotalCps`
* the workload was multi-hot but no single document individually crossed the single-doc threshold

### If 1 document appears

That is the cleanest “single hot document” case.

### If 2 documents appear

That means both documents independently passed the single-doc gate.

For \(N=128\), that is possible only if both are each near or above ~44.4% of all events in the window.

## Practical tuning guidance

### More sensitive / investigation mode

Good for debugging and lab work:

```js
minDocCps: 2,
minTotalCps: 10
```

### More conservative / production screening

Good for reducing false positives:

```js
minDocCps: 5,
minTotalCps: 20
```

### Very permissive / test mode

Good for synthetic testing:

```js
minDocCps: 1,
minTotalCps: 0
```

## Summary

This script is a focused detector for documents that are individually hot enough to explain high spread disparity in a recent window.

Its decision rule is:

* high enough share of all window events
* high enough per-document throughput
* high enough total-window throughput

That makes it simple, explainable, and practical for identifying strong single-document hotspot candidates.


---

## Sources

- [EP: Automatically detect and mitigate hot docs](https://docs.google.com/document/d/1mHBMjpeYnQKJyxAWjhUL7OBhGJ2733uakUWZZCsp5p4)
