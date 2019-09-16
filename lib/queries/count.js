const { getConnection, MAIN_DB } = require('../database');
const { makeBasePipeline, createAddOn, createIdMetadata, hash, createLabelMetadata } = require('./helpers');

async function query(conceptId, periodo, params, group) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);

    const self = conceptId.startsWith('!');
    conceptId = self ? conceptId.substr(1) : conceptId;

    const { pipeline, needUnwind } = makeBasePipeline(conceptId, periodo, params, { forceUnwind: !!group, self });
    const metadataID = createIdMetadata(group);
    const addOns = group ? createAddOn(group, params) : [];

    const countKey = needUnwind ? 1 : '$total';

    const $pipeline = [
        ...pipeline,
        ...addOns,
        {
            $group: {
                ...metadataID,
                total: { $sum: countKey },
                exact: {
                    $sum: {
                        $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: countKey, else: 0 }
                    }
                }
            },
        },
        {
            $project: {
                _id: 1,
                label: createLabelMetadata(group),
                value: {
                    total: '$total',
                    exact: '$exact'
                }
            }
        }
    ];
    const results = await PrestacionesTx.aggregate($pipeline).toArray();
    results.forEach(r => {
        r.hashId = hash(r._id);
    });
    return results;
}

const initial = () => ({
    exact: 0,
    total: 0
})

const reducer = (acc, value) => {
    return {
        total: acc.total + value.total,
        exact: acc.exact + value.exact
    };
}

const transform = (value) => {
    return {
        exact: value.exact,
        total: value.total,
        children: value.total - value.exact
    }
};


module.exports = {
    name: 'count',
    query,
    reducer,
    initial,
    transform,
    cache: true,
    unwind: false,
    split: true
}