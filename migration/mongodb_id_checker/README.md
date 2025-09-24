# mongodb_id_check

# Helper tool to identify the type used for _id on user collections

## What it does

The goal of this tool is to report the number of documents for each possible type for _id present in each user collection. System collections like local, admin or system are ignored.

## How to Run

NOTE: This script is meant to run on Dev or Staging environments, *NOT IN PRODUCTION*. This is assuming that the data schema and _id field type should be consistent across all environments and to avoid querying all collections by _id on a production environment

This is a simple javascript script that should be run against a mongosh shell like in the example below:

> mongosh "mongodb://my-cluster-shard-00-00.0mfal.mongodb.net:27017,my-cluster-shard-00-01.0mfal.mongodb.net:27017,my-cluster-shard-00-02.0mfal.mongodb.net:27017/?replicaSet=atlas-wl250a-shard-0" --tls --authenticationDatabase admin --username user --password my_password --quiet --norc mongodb_id_checker.js


## Output

```
-----------------------------------------------------------
Helper tool to identify type of _id on user collections
-----------------------------------------------------------

...
==============================================
= Namespace: test.test1
==============================================
84 * ObjectId
   Other types:
==============================================
= Namespace: test.test2
==============================================
637 * ObjectId
   Other types:
==============================================
= Namespace: test.test3
==============================================
14784 * ObjectId
   Other types:
...
Not inspecting admin DB
...
Not inspecting config DB
...
Not inspecting local DB
...
==============================================
= Namespace: test.nonObjectIdColl
==============================================
   Other types:
   1       Double
   1       String

```