const { getConnection, MAIN_DB } = require('../database');

async function shortQuery(conceptId, start, params) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);

    const filtros = {};

    if (params.organizacion) {
        filtros['organizacion.id'] = params.organizacion;
    }

    if (params.profesional) {
        filtros['profesional.id'] = params.profesional;
    }

    if (start) {
        filtros['start'] = start.toDate();
    }

    const pipeline = [
        {
            $match: {
                ...filtros,
                'concepto.conceptId': conceptId
            }
        },
        { $unwind: '$registros' },
        {
            $group: {
                _id: '$registros.term',
                count: { $sum: 1 }
            }
        },
        {
            $project: {
                _id: 0,
                term: '$_id',
                count: 1
            }
        }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    return results;
}


async function longQuery(conceptId, start, end, params) {
    const db = await getConnection();
    const PrestacionesTx = db.collection(MAIN_DB);

    const filtros = {};
    const postFiltros = {};

    if (params.organizacion) {
        filtros['organizacion.id'] = params.organizacion;
    }

    if (params.profesional) {
        filtros['profesional.id'] = params.profesional;
    }

    if (start) {
        filtros['start'] = { $gte: start.startOf('month').toDate() };
        postFiltros['start'] = { $gte: start.startOf('day').toDate() };
    }

    if (end) {
        filtros['end'] = { $lte: end.endOf('month').toDate() };
        postFiltros['end'] = { $lte: end.endOf('day').toDate() };
    }

    const pipeline = [
        {
            $match: {
                ...filtros,
                'concepto.conceptId': conceptId
            }
        },
        { $unwind: '$registros' },
        { $match: postFiltros },
        {
            $group: {
                _id: '$registros.term',
                count: { $sum: 1 }
            }
        },
        {
            $project: {
                _id: 0,
                term: '$_id',
                count: 1
            }
        }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    return results;
}

const initial = () => ({})

const reducer = (acc, items) => {
    items.forEach(item => {
        if (!acc[item.term]) {
            acc[item.term] = 0;
        }
        acc[item.term] += item.count;

    })
    return acc;
}

module.exports = {
    shortQuery,
    longQuery,
    reducer,
    initial,
    cache: false,
}