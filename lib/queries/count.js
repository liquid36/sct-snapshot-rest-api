const { getConnection, MAIN_DB } = require('../database');
const { makeBasePipeline } = require('./helpers');

async function query(conceptId, periodo, params) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);
    const { pipeline, needUnwind } = makeBasePipeline(conceptId, periodo, params);
    const countKey = needUnwind ? 1 : '$total';

    const $pipeline = [
        ...pipeline,
        {
            $group: {
                _id: null,
                total: { $sum: countKey },
                exact: {
                    $sum: {
                        $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: countKey, else: 0 }
                    }
                },
            }
        }
    ];
    const results = await PrestacionesTx.aggregate($pipeline).toArray();
    if (results.length > 0) {
        return {
            total: results[0].total,
            exact: results[0].exact,
            children: results[0].total - results[0].exact,
        }
    } else {
        return {
            exact: 0,
            total: 0,
            children: 0
        }
    }
}

const initial = () => ({
    exact: 0,
    total: 0,
    children: 0
})

const reducer = (acc, value) => {
    return {
        total: acc.total + value.total,
        exact: acc.exact + value.exact,
        children: acc.children + value.children
    };
}

module.exports = {
    query,
    reducer,
    initial,
    cache: true,
}