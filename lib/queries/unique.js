const { getConnection, MAIN_DB } = require('../database');
const { makeBasePipeline } = require('./helpers');

async function query(conceptId, perdiodo, params) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);
    const { pipeline } = makeBasePipeline(conceptId, perdiodo, params, { forceUnwind: true });
    const $pipeline = [
        ...pipeline,
        {
            $group: {
                _id: null,
                pacientes: { $addToSet: { $toString: '$registros.paciente.id' } }
            }
        }
    ];
    const results = await PrestacionesTx.aggregate($pipeline).toArray();
    if (results.length > 0) {
        return results[0].pacientes;
    } else {
        return [];
    }
}

const initial = () => new Set()

const reducer = (acc, value) => {
    value.forEach(v => acc.add(v));
    return acc;
};

const transform = (value) => value.size

module.exports = {
    query,
    reducer,
    initial,
    transform,
    cache: true,
}