const { getConnection, MAIN_DB } = require('../database');
const { makeBasePipeline, createAddOn, createIdMetadata, hash, createLabelMetadata } = require('./helpers');

async function query(conceptId, perdiodo, params, group) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);
    const { pipeline } = makeBasePipeline(conceptId, perdiodo, params, { forceUnwind: true });

    const metadataID = createIdMetadata(group);
    const addOns = group ? createAddOn(group, params) : [];

    const $pipeline = [
        ...pipeline,
        ...addOns,
        {
            $group: {
                ...metadataID,
                value: { $addToSet: { $toString: '$registros.paciente.id' } }
            }
        },
        {
            $project: {
                _id: 1,
                label: createLabelMetadata(group),
                value: 1
            }
        }
    ];
    const results = await PrestacionesTx.aggregate($pipeline).toArray();
    results.forEach(r => {
        r.hashId = hash(r._id);
        r.value = r.value;
    });
    return results;
}

const initial = () => new Set()

const reducer = (acc, value) => {
    console.log(acc, value)
    const set = new Set([...acc, ...value]);
    return [...set];
};

const transform = (value) => value.length

module.exports = {
    name: 'unique',
    query,
    reducer,
    initial,
    transform,
    cache: true,
    unwind: true,
    split: true
}