const moment = require('moment');
const express = require('express');
const router = express.Router();
const MongoClient = require('mongodb').MongoClient;

const { execQuery } = require('../lib/analytica');
const { makePattern } = require('../lib/util');

const mongoConnection = process.env['ANDES_DB_CONN'] || process.env['MONGO_DB_CONN'] || "localhost:27017";
const databases = {};

const ObjectID = require('bson').ObjectID;

function toArray(item) {
    return Array.isArray(item) ? item : [item];
}

router.post('/analytics/:visualization', async function (req, res) {
    let { target, filter, visualization, group } = req.body;
    target = toArray(target);
    group = group && toArray(group);
    filter = filter || {};
    visualization = req.params.visualization;

    const rs = await execQuery(visualization, target, filter, group);
    return res.json(rs);

});


//--------------------------------------------------------------------------
//--------------------------------------------------------------------------
//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

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

function getDate(date) {
    return date ? moment(date) : null;
}


router.get('/organizaciones', async function (req, res) {
    const search = req.query.search;
    const expWord = makePattern(search);

    const db = await getConnection();
    const Organizaciones = db.collection('organizaciones');
    const orgs = await Organizaciones.find({ nombre: { $regex: expWord, $options: 'i' } }).toArray();
    return res.json(orgs);
});

router.get('/semanticTags', async function (req, res) {
    const search = req.query.search;
    const expWord = makePattern(search);

    const db = await getConnection();
    const semanticTags = db.collection('semanticTags');
    const items = await semanticTags.find({ _id: { $regex: expWord, $options: 'i' } }).toArray();
    return res.json(items);
});

router.post('/rup/demografia', async function (req, res) {
    const db = await getConnection();
    const PrestacionesTx = db.collection('prestaciontx2');
    const conceptId = req.body.conceptId;
    const rangoEtario = req.body.rango;

    const filtros = {};
    const postFiltros = {};
    const start = getDate(req.body.start);
    const end = getDate(req.body.end);
    const organizacion = req.body.organizacion ? ObjectID(req.body.organizacion) : null;

    if (organizacion) {
        filtros['organizacion.id'] = organizacion;
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
                $or: [
                    { 'concepto.conceptId': conceptId },
                    { 'concepto.statedAncestors': conceptId }
                ]

            }
        },
        { $unwind: '$registros' },
        { $match: postFiltros },
        {
            $facet: {
                rangoEtario: [
                    {
                        $bucket: {
                            groupBy: "$registros.paciente.edad.edad",
                            boundaries: rangoEtario, // [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90],
                            default: 100,
                            output: {
                                "pacientes": { $push: "$registros.paciente" },
                            }
                        }
                    },
                    { $unwind: '$pacientes' },
                    { $group: { _id: { decada: '$_id', sexo: '$pacientes.sexo' }, count: { $sum: 1 } } },
                    {
                        $project: {
                            _id: 0,
                            decada: '$_id.decada',
                            sexo: '$_id.sexo',
                            count: '$count'
                        }
                    }
                ],
                localidades: [
                    {
                        $group: {
                            _id: '$registros.paciente.localidad',
                            count: { $sum: 1 },
                            exact: {
                                $sum: {
                                    $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: 1, else: 0 }
                                }
                            },
                        }
                    },
                    {
                        $project: {
                            _id: 1,
                            nombre: '$_id',
                            count: 1,
                            exact: 1
                        }
                    },
                    { $sort: { count: -1 } }
                ],
                profesionales: [
                    {
                        $group: {
                            _id: '$registros.profesional.id',
                            count: { $sum: 1 },
                            exact: {
                                $sum: {
                                    $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: 1, else: 0 }
                                }
                            },
                            profesional: { $first: '$registros.profesional' }
                        }
                    },
                    {
                        $project: {
                            _id: 1,
                            nombre: { $concat: ['$profesional.apellido', ' ', '$profesional.nombre'] },
                            count: 1,
                            exact: 1
                        }
                    },
                    { $sort: { count: -1 } }
                ],
                profesionales_primera: [
                    {
                        $group: {
                            _id: '$registros.paciente.id',
                            profesional: { $first: '$registros.profesional' }
                        },
                    },
                    {
                        $group: {
                            _id: '$profesional.id',
                            primera: { $sum: 1 }
                        }
                    },

                ],
                profesionales_paciente: [
                    {
                        $group: {
                            _id: '$registros.profesional.id',
                            pacientes: { $addToSet: '$registros.paciente.id' }
                        }
                    },
                    {
                        $project: {
                            _id: true,
                            pacientes: { $size: '$pacientes' }
                        }
                    }

                ],
                prestacion: [
                    {
                        $group: {
                            _id: '$registros.tipoPrestacion.conceptId',
                            count: { $sum: 1 },
                            exact: {
                                $sum: {
                                    $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: 1, else: 0 }
                                }
                            },
                            prestacion: { $first: '$registros.tipoPrestacion' }
                        }
                    },
                    { $project: { _id: 1, nombre: '$prestacion.term', count: 1, exact: 1 } },
                    { $sort: { count: -1 } }
                ],
                prestacion_primera: [
                    {
                        $group: {
                            _id: '$registros.paciente.id',
                            prestacion: { $first: '$registros.tipoPrestacion' }
                        },
                    },
                    {
                        $group: {
                            _id: '$prestacion.conceptId',
                            primera: { $sum: 1 }
                        }
                    },

                ],
                prestacion_paciente: [
                    {
                        $group: {
                            _id: '$registros.tipoPrestacion.conceptId',
                            pacientes: { $addToSet: '$registros.paciente.id' }
                        }
                    },
                    {
                        $project: {
                            _id: true,
                            pacientes: { $size: '$pacientes' }
                        }
                    }

                ],
                organizaciones: [
                    {
                        $group: {
                            _id: '$organizacion.id',
                            count: { $sum: 1 },
                            exact: {
                                $sum: {
                                    $cond: { if: { $eq: ['$concepto.conceptId', conceptId] }, then: 1, else: 0 }
                                }
                            },
                            organizacion: { $first: '$organizacion' }
                        }
                    },
                    { $project: { _id: 1, nombre: '$organizacion.nombre', count: 1, exact: 1 } },
                    { $sort: { count: -1 } }
                ],
                organizaciones_primera: [
                    {
                        $group: {
                            _id: '$registros.paciente.id',
                            organizacion: { $first: '$organizacion' }
                        },
                    },
                    {
                        $group: {
                            _id: '$organizacion.id',
                            primera: { $sum: 1 }
                        }
                    },

                ],
                organizaciones_paciente: [
                    {
                        $group: {
                            _id: '$organizacion.id',
                            pacientes: { $addToSet: '$registros.paciente.id' }
                        }
                    },
                    {
                        $project: {
                            _id: true,
                            pacientes: { $size: '$pacientes' }
                        }
                    }

                ],
                // fechas: [
                //     { $addFields: { mes: { $dateToString: { date: '$registros.fecha', format: '%Y-%m' } } } },
                //     {
                //         $group: {
                //             _id: '$mes',
                //             count: { $sum: 1 },
                //         }
                //     },
                //     { $project: { _id: 1, nombre: '$_id', count: 1 } },
                //     { $sort: { _id: 1 } }

                // ]
            }
        }
    ];
    const results = await PrestacionesTx.aggregate(pipeline, { allowDiskUse: true }).toArray();
    const data = results[0];
    combine(data.profesionales, data.profesionales_primera, 'primera');
    combine(data.profesionales, data.profesionales_paciente, 'pacientes');

    combine(data.prestacion, data.prestacion_primera, 'primera');
    combine(data.prestacion, data.prestacion_paciente, 'pacientes');

    combine(data.organizaciones, data.organizaciones_primera, 'primera');
    combine(data.organizaciones, data.organizaciones_paciente, 'pacientes');

    delete data['profesionales_primera'];
    delete data['profesionales_paciente'];
    delete data['prestacion_primera'];
    delete data['prestacion_paciente'];
    delete data['organizaciones_primera'];
    delete data['organizaciones_paciente'];
    return res.json(results[0]);
});

function combine(listA, listB, key) {
    listA.forEach((item) => {
        itemB = listB.find(i => String(i._id) === String(item._id));
        item[key] = itemB ? itemB[key] : 0;
    });
}

router.post('/rup/cluster', async function (req, res) {
    const db = await getConnection();
    const PrestacionesTx = db.collection('prestaciontx2');
    const conceptId = req.body.conceptId;
    const semanticTags = req.body.semanticTags || ['trastorno'];
    const pipeline = [
        {
            $match: {
                $or: [
                    { 'concepto.conceptId': conceptId },
                    { 'concepto.statedAncestors': conceptId }
                ]

            }
        },
        { $unwind: '$registros' },
        { $group: { '_id': '$registros.paciente.id' } }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray()
    const ids = results.map(e => ObjectID(e._id));

    const pipeline2 = [
        {
            $match: {
                'registros.paciente.id': { $in: ids }
            }
        },
        { $match: { 'concepto.semanticTag': { $in: semanticTags }, 'concepto.conceptId': { $ne: conceptId } } },
        { $group: { '_id': '$concepto.conceptId', 'nombre': { $first: '$concepto.term' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } }
    ];
    const concepts = await PrestacionesTx.aggregate(pipeline2).toArray()
    return res.json(concepts);
});

router.post('/rup/maps', async function (req, res) {
    const db = await getConnection();
    const PrestacionesTx = db.collection('prestaciontx2');
    const conceptId = req.body.conceptId;

    const pipeline = [
        {
            $match: {
                $or: [
                    { 'concepto.conceptId': conceptId },
                    { 'concepto.statedAncestors': conceptId }
                ]

            }
        },
        { $unwind: '$registros' },
        { $match: { 'registros.paciente.coordenadas': { $ne: null } } },
        { $project: { 'coordenadas': '$registros.paciente.coordenadas' } }

    ];
    function desvio() {
        return (Math.floor(Math.random() * 40000) - 20000) / 1000000;
    }
    const results = await PrestacionesTx.aggregate(pipeline).toArray()

    const r = results.map(point => {
        if (point.coordenadas.aprox) {
            return {
                lat: point.coordenadas.lat + desvio(),
                lng: point.coordenadas.lng + desvio()
            }
        } else {
            return {
                lat: point.coordenadas.lat,
                lng: point.coordenadas.lng
            }
        }
    });
    res.json(r);

});


async function storeInCache(organizacion, start, end, conceptId, result) {
    const db = await getConnection();
    const cache = db.collection('cache');
    start = start ? start.toDate() : null;
    end = end ? end.toDate() : null;

    const cacheObj = {
        organizacion,
        start,
        end,
        conceptId: conceptId,
        value: result,
        createdAt: new Date()
    }

    await cache.insert(cacheObj);
}

async function findInCache(organizacion, start, end, concepts) {
    const db = await getConnection();
    const cache = db.collection('cache');

    start = start ? start.toDate() : null;
    end = end ? end.toDate() : null;

    const data = await cache.find({
        organizacion,
        start,
        end,
        conceptId: { $in: concepts }
    }).toArray();
    const dt = {};
    data.forEach(item => dt[item.conceptId] = item.value);
    return dt;
}

/*
-38.951929, -68.059161
-38.951739, -68.073225

1KM = 0.020000
20KM = 2

*/
module.exports = router;


/**
 *
 * Query de concepto con tipo number
 *

 db.getCollection('prestaciontx2').aggregate([
{ $match: { 'registros.valorType' : 'number' } },
{ $group: { _id: '$concepto.conceptId', 'concepto': { $first: '$concepto' } }  },
{ $replaceRoot: { newRoot: '$concepto' } },
{ $sort: { 'fsn': 1 } }
])

db.getCollection('prestaciontx2').aggregate([
{ $group: { _id: '$concepto.semanticTag', 'total': { $sum: 1 } }  },
{ $out: 'semanticTags' }
])

 */


/**
 *
 * PARA LA BASE DE DATOS
 *
 * NO OBJECTID
 * PROFESIONAL NOMBRE TODO JUNTO
 * EDAD PACIENTE EN SEMANAS
 */