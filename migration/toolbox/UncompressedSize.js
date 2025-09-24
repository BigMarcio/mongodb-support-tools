// List to exclude system databases  
const systemDbs = ['admin', 'local', 'config'];  
  
let totalDataSize = 0;  
let totalIndexSize = 0;  
  
const dbNames = db.adminCommand('listDatabases').databases  
    .map(database => database.name)  
    .filter(name => !systemDbs.includes(name));  
  
dbNames.forEach(dbName => {  
    const currentDb = db.getSiblingDB(dbName);  
    currentDb.getCollectionNames().forEach(collName => {  
        try {  
            const stats = currentDb.getCollection(collName).stats();  
            totalDataSize += stats.size;  
            totalIndexSize += stats.totalIndexSize;  
        } catch (e) {  
            print("Error processing " + dbName + "." + collName + ": " + e.message);  
        }  
    });  
});  
  
print("Total uncompressed data size (MB): " + (totalDataSize / (1024 * 1024)).toFixed(2));  
print("Total uncompressed index size (MB): " + (totalIndexSize / (1024 * 1024)).toFixed(2));  
