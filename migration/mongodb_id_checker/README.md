# MongoDB _id checker

# Helper tool to identify if the user collections have non objectId as _id

## What it does

The goal of this tool is to report the collections having non objectId as _id and count the number of documents. System collections like local, admin or system are ignored.
After identify those collections _id check will sample 10 documents in natural order to validate if they are written on disk in order or random.

## How to Run

NOTE: This script is meant to run on Dev or Staging environments, *NOT IN PRODUCTION*. This is assuming that the data schema and _id field type should be consistent across all environments and to avoid querying all collections by _id on a production environment

This is a simple javascript script that should be run against a mongosh shell like in the example below:

> mongosh "mongodb://my-cluster-shard-00-00.0mfal.mongodb.net:27017,my-cluster-shard-00-01.0mfal.mongodb.net:27017,my-cluster-shard-00-02.0mfal.mongodb.net:27017/?replicaSet=atlas-wl250a-shard-0" --tls --authenticationDatabase admin --username user --password my_password --quiet --norc mongodb_id_checker.js


## Output

```
namespace,type,count
foo.bar3,String,500
foo.bar3,ObjectId,500
foo.bar2,String,1000
foo.bar1,ObjectId,1000

Namespaces with at least one non-ObjectId _id:

Namespace: foo.bar3
{
  _id: ObjectId('68d44737d49f994f913557a3')
}
{
  _id: 'nC1lfJRZuRpqSxis'
}
{
  _id: ObjectId('68d44737d49f994f913557a4')
}
{
  _id: '9uTkpHLNAbyVFA6P'
}
{
  _id: ObjectId('68d44737d49f994f913557a5')
}
{
  _id: 'Hor4ajNbWdWog8lo'
}
{
  _id: ObjectId('68d44737d49f994f913557a6')
}
{
  _id: 'DYpLsQTj3P4QcKSh'
}
{
  _id: ObjectId('68d44737d49f994f913557a7')
}
{
  _id: 'g5RJLvecIS5Fj1Pm'
}
Namespace: foo.bar2
{
  _id: 'hUBcBEU3M3TTkdEl'
}
{
  _id: 'mrKm3XDHbG0TgIwB'
}
{
  _id: 'OjfZlSBBSHRZKmAG'
}
{
  _id: 'YX7MU06dwsT3hWhS'
}
{
  _id: 'O5dTqnhC2Qty9VWm'
}
{
  _id: '0JRLLy8IB0gm4smA'
}
{
  _id: 'gJoNGEHPCuUZrnZz'
}
{
  _id: 'qn31QLJBDIQDJsK0'
}
{
  _id: 'Dj0SXcKfTZWJ3DKN'
}
{
  _id: 'ULd8TdARpeItAD2i'
}
```

### License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

DISCLAIMER
----------
Please note: all tools/ scripts in this repo are released for use "AS IS" **without any warranties of any kind**,
including, but not limited to their installation, use, or performance.  We disclaim any and all warranties, either 
express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness 
for a particular purpose.  We do not warrant that the technology will meet your requirements, that the operation 
thereof will be uninterrupted or error-free, or that any errors will be corrected.

Any use of these scripts and tools is **at your own risk**.  There is no guarantee that they have been through 
thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with 
their use.

You are responsible for reviewing and testing any scripts you run *thoroughly* before use in any non-testing 
environment.

Thanks,  
The MongoDB Support Team