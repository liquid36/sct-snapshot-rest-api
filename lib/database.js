const MongoClient = require('mongodb').MongoClient;

const mongoConnection = process.env['ANDES_DB_CONN'] || process.env['MONGO_DB_CONN'] || "localhost:27017";
const databases = {};


const getConnection = async function () {
    try {
        if (databases['andes']) {
            return databases['andes'];
        } else {
            const db = MongoClient.connect(mongoConnection);
            databases['andes'] = db;
            return db;
        }
    } catch (err) {
        console.warn(err.message);
        process.exit();
    }
}

async function ensureIndex() {
    const db = await getConnection();
    const cache = db.collection('cache');
    cache.ensureIndex({
        conceptId: 1,
        cache_type: 1,
        start: 1,
        hash_key: 1
    });
    cache.ensureIndex({ 'lastUse': 1 }, { expireAfterSeconds: 86400 });
}

ensureIndex();

module.exports.getConnection = getConnection;
module.exports.MAIN_DB = 'prestaciontx2';
module.exports.CACHE_DB = 'cache';