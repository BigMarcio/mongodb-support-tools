printjson('-----------------------------------------------------------');
printjson('Helper tool to identify type of _id on user collections');
printjson('-----------------------------------------------------------');

printjson(" ")
const systemDBs = ["admin", "config", "local"];  
db.getMongo().getDBNames().forEach(function (d) {
    if (systemDBs.includes(d)) {  
        return; // Skip system DBs  
    }  
    const curr_db = db.getMongo().getDB(d);  

    curr_db.getCollectionNames().forEach(function (coll) {
        var count = -1
        printjson('==============================================');
        printjson('= Namespace: ' + d + '.' + coll );
        printjson('==============================================');
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 7 } } )
        if (count > 0) {
            printjson(count+" * ObjectId ")
        }
        printjson('   Other types:');
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 1 } } )
        if (count > 0) {
            printjson('   '+count+"       Double ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 2 } } )
        if (count > 0) {
            printjson('   '+count+"       String ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 3 } } )
        if (count > 0) {
            printjson('   '+count+"       Object ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 5 } } )
        if (count > 0) {
            printjson('   '+count+"       Binary data ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 6 } } )
        if (count > 0) {
            printjson('   '+count+"       Undefined ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 8 } } )
        if (count > 0) {
            printjson('   '+count+"       Boolean ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 9 } } )
        if (count > 0) {
            printjson('   '+count+"       Date ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 10 } } )
        if (count > 0) {
            printjson('   '+count+"       Null ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 11 } } )
        if (count > 0) {
            printjson('   '+count+"       Regular Expression ",)
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 12 } } )
        if (count > 0) {
            printjson('   '+count+"       DBPointer ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 13 } } )
        if (count > 0) {
            printjson('   '+count+"       Javascript ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 14 } } )
        if (count > 0) {
            printjson('   '+count+"       Symbol ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 15 } } )
        if (count > 0) {
            printjson('   '+count+"       Javascript code ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 16 } } )
        if (count > 0) {
            printjson('   '+count+"       32-bit integer ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 18 } } )
        if (count > 0) {
            printjson('   '+count+"       64-bit integer ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 19 } } )
        if (count > 0) {
            printjson('   '+count+"       Decimal128 ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : -1 } } )
        if (count > 0) {
            printjson('   '+count+"       Min key ")
        }
        count = curr_db.getCollection(coll).countDocuments( { "_id" : { $type : 127 } } )
        if (count > 0) {
            printjson('   '+count+"       Max key ")
        }   

    });
});