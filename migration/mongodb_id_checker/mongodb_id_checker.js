print("namespace,type,count");  
  
const systemDBs = ["admin", "config", "local"];  
const typesMap = {  
    "1": "Double",  
    "2": "String",  
    "3": "Object",  
    "5": "Binary data",  
    "6": "Undefined",  
    "7": "ObjectId",  
    "8": "Boolean",  
    "9": "Date",  
    "10": "Null",  
    "11": "Regular Expression",  
    "12": "DBPointer",  
    "13": "Javascript",  
    "14": "Symbol",  
    "15": "Javascript code w. scope",  
    "16": "32-bit integer",  
    "18": "64-bit integer",  
    "19": "Decimal128",  
    "-1": "Min key",  
    "127": "Max key"  
};  
  
let nonObjectIdNamespaces = [];  
  
db.getMongo().getDBNames().forEach(function (dbName) {  
    if (systemDBs.includes(dbName)) {  
        return;  
    }  
    const curr_db = db.getMongo().getDB(dbName);  
  
    curr_db.getCollectionNames().forEach(function (collName) {  
        let hasNonObjectId = false;  
        Object.keys(typesMap).forEach(function(typeNum) {  
            var count = curr_db.getCollection(collName).countDocuments({"_id": { $type: Number(typeNum) }});  
            if (count > 0) {   
                print([dbName + "." + collName, typesMap[typeNum], count].join(","));  
                    if (Number(typeNum) != 7) {
                        hasNonObjectId = true;
                    }
            }  
        });  
        // If any _id is not ObjectId, track the namespace for second step  
        if (hasNonObjectId) {  
            nonObjectIdNamespaces.push({ dbName: dbName, collName: collName });  
        }  
    });  
});  
  
// Second phase: list the first 10 _id for those namespaces  
print("\nNamespaces with at least one non-ObjectId _id:\n");  
  
nonObjectIdNamespaces.forEach(function(ns) {  
    const curr_db = db.getMongo().getDB(ns.dbName);  
    print("Namespace: " + ns.dbName + "." + ns.collName);  
    curr_db.getCollection(ns.collName)  
        .find({}, { _id: 1 })  
        .sort({ $natural: 1 })  
        .limit(10)  
        .forEach(function (doc) {  
            printjson(doc);  
        });  
});  
