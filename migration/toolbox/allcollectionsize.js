// List to exclude system databases  
const systemDbs = ['admin', 'local', 'config'];  
  
const dbNames = db.adminCommand('listDatabases').databases  
    .map(database => database.name)  
    .filter(name => !systemDbs.includes(name));  
  
print("Database\tCollection\tSize (MB)");  
  
dbNames.forEach(dbName => {  
    const currentDb = db.getSiblingDB(dbName);  
    currentDb.getCollectionNames().forEach(collName => {  
        try {  
            const stats = currentDb.getCollection(collName).stats();  
            const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);  
            print(dbName + "\t" + collName + "\t" + sizeMB);  
        } catch (e) {  
            print(dbName + "\t" + collName + "\t" + "error: " + e.message);  
        }  
    });  
}); 
