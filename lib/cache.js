const { getConnection, CACHE_DB } = require('./database');
const { FILTER_AVAILABLE } = require('./util');
const base64 = require('base-64');

async function storeInCache(cache_type, conceptId, start, params, result) {
    const db = await getConnection();
    const cache = db.collection(CACHE_DB);
    start = start ? start.toDate() : null;

    const cacheObj = {
        cache_type,
        hash_key: hash(params),
        start,
        conceptId: conceptId,
        value: result,
        lastUse: new Date(),
        used: 1
    }

    await cache.insert(cacheObj);
}

async function restoreFromCache(cache_type, concepts, start, end, params) {
    const db = await getConnection();
    const cache = db.collection(CACHE_DB);

    start = start ? start.toDate() : null;
    end = end ? end.toDate() : null;

    const data = await cache.find({
        hash_key: hash(params),
        cache_type,
        start: { $gte: start, $lte: end },
        conceptId: { $in: concepts }
    }).toArray();
    const dt = {};
    data.forEach(item => {
        if (!dt[item.conceptId]) dt[item.conceptId] = [];
        dt[item.conceptId].push(item);
    });
    return dt;
}

async function touchCache(item) {
    const db = await getConnection();
    const cache = db.collection(CACHE_DB);
    cache.update({ _id: item._id }, { $set: { lastUse: new Date() }, $inc: { used: 1 } });
}

function hash(params) {
    const hashTarget = {};
    FILTER_AVAILABLE.forEach(filter => {
        const name = filter.name;
        hashTarget[name] = params[name];
    });
    return base64.encode(JSON.stringify(hashTarget));
}

module.exports.storeInCache = storeInCache;
module.exports.restoreFromCache = restoreFromCache;
module.exports.touchCache = touchCache;