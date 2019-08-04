const moment = require('moment');
const express = require('express');
const router = express.Router();
const MongoClient = require('mongodb').MongoClient;

const mongoConnection = process.env['MONGO_DB_CONN'] || "localhost:27017";
const databases = {};

const ObjectID = require('bson').ObjectID;


const getConnection = async function () {
    try {
        if (databases['andes']) {
            return databases['andes'];
        } else {
            const db = MongoClient.connect("mongodb://" + mongoConnection + "/andes");
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

function makeFacet(sctid, sumador = '$total') {
    return [
        {
            $match: {
                $or: [
                    { 'concepto.conceptId': sctid },
                    { 'concepto.statedAncestors': sctid }
                ]
            },
        }, {
            $group: {
                _id: null,
                total: { $sum: sumador },
                exact: {
                    $sum: {
                        $cond: { if: { $eq: ['$concepto.conceptId', sctid] }, then: sumador, else: 0 }
                    }
                },
            }
        }
    ];
}

function combinarConceptos(conceptsIds, a, b) {
    const data = {};
    conceptsIds.forEach(c => {
        data[c] = {
            exact: a[c].exact + b[c].exact,
            total: a[c].total + b[c].total,
            children: a[c].children + b[c].children
        }
    });
    return data;
}

function contarConceptos(conceptsIds, results) {
    if (results) {
        const data = {};
        results = results[0];
        Object.keys(results).forEach(key => {
            if (results[key][0]) {
                data[key] = results[key][0];
                data[key].children = data[key].total - data[key].exact;
                delete data[key]['_id'];
            } else {
                data[key] = {
                    exact: 0,
                    total: 0,
                    children: 0
                }
            }
        })
        return data;
    } else {
        const data = {};
        conceptsIds.forEach(c => {
            data[c] = {
                exact: 0,
                total: 0,
                children: 0
            }
        });
        return data;
    }
}

async function makeUnwindQuery(conceptsIds, start, end, organizacion) {
    const db = await getConnection();
    const PrestacionesTx = db.collection('prestaciontx2');
    const $match = {
        $or: [
            { 'concepto.conceptId': { $in: conceptsIds } },
            { 'concepto.statedAncestors': { $in: conceptsIds } }
        ]
    };
    if (start) {
        $match['start'] = { $lte: start.toDate() };
    }
    if (end) {
        $match['end'] = { $gt: end.toDate() };
    }
    if (organizacion) {
        $match['organizacion.id'] = organizacion;
    }

    const $facet = {};
    conceptsIds.forEach(c => {
        $facet[c] = makeFacet(c, 1);
    });

    const pipeline = [
        { $match: $match },
        { $unwind: '$registros' },
        { $match: { fecha: { $gte: start.toDate(), $lte: end.toDate() } } },
        { $facet: $facet }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    return contarConceptos(conceptsIds, results);
}

async function makeShortQuery(conceptsIds, start, end, organizacion) {
    const db = await getConnection();
    const PrestacionesTx = db.collection('prestaciontx2');
    const $match = {
        $or: [
            { 'concepto.conceptId': { $in: conceptsIds } },
            { 'concepto.statedAncestors': { $in: conceptsIds } }
        ]
    };
    if (start) {
        $match['start'] = { $gte: start.toDate() };
    }
    if (end) {
        $match['end'] = { $lte: end.toDate() };
    }
    if (organizacion) {
        $match['organizacion.id'] = organizacion;
    }

    const $facet = {};
    conceptsIds.forEach(c => {
        $facet[c] = makeFacet(c);
    });

    const pipeline = [
        { $match: $match },
        { $facet: $facet }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    return contarConceptos(conceptsIds, results);
}

router.post('/rup', async function (req, res) {
    const conceptsIds = req.body.concepts;
    const start = getDate(req.body.start);
    const end = getDate(req.body.end);
    const organizacion = req.body.organizacion ? ObjectID(req.body.organizacion) : null;

    const conceptInCache = await findInCache(organizacion, start, end, conceptsIds);
    const realConcepts = conceptsIds.filter(c => !conceptInCache[c]);

    if (realConcepts.length > 0) {
        let ini, fin, middleStart, middleEnd;
        if (start) {
            ini = start.startOf('day').clone();
            middleStart = start.clone().endOf('month');
        }

        if (end) {
            fin = end.endOf('day').clone();
            middleEnd = end.clone().startOf('month');
        }
        let data = await makeShortQuery(realConcepts, middleStart, middleEnd, organizacion);
        if (ini) {
            const dd = await makeUnwindQuery(realConcepts, ini, middleStart, organizacion);
            data = combinarConceptos(realConcepts, data, dd);
        }

        if (fin) {
            const dd = await makeUnwindQuery(realConcepts, middleEnd, fin, organizacion);
            data = combinarConceptos(realConcepts, data, dd);
        }
        conceptsIds.forEach(key => {
            if (!data[key]) data[key] = conceptInCache[key];
        });

        res.json(data);

        Object.keys(data).forEach(key => {
            storeInCache(organizacion, start, end, key, data[key]);
        });
    } else {
        res.json(conceptInCache);
    }


});

var defaultDiacriticsRemovalMap = [
    { 'base': 'a', 'letters': /[\u00E1\u00E2\u00E3\u00E4\u00E5\u0101\u0103\u0105\u01CE\u01FB\u00C0\u00C4]/g },
    { 'base': 'ae', 'letters': /[\u00E6\u01FD]/g },
    { 'base': 'c', 'letters': /[\u00E7\u0107\u0109\u010B\u010D]/g },
    { 'base': 'd', 'letters': /[\u010F\u0111\u00F0]/g },
    { 'base': 'e', 'letters': /[\u00E8\u00E9\u00EA\u00EB\u0113\u0115\u0117\u0119\u011B]/g },
    { 'base': 'f', 'letters': /[\u0192]/g },
    { 'base': 'g', 'letters': /[\u011D\u011F\u0121\u0123]/g },
    { 'base': 'h', 'letters': /[\u0125\u0127]/g },
    { 'base': 'i', 'letters': /[\u00ED\u00EC\u00EE\u00EF\u0129\u012B\u012D\u012F\u0131]/g },
    { 'base': 'ij', 'letters': /[\u0133]/g },
    { 'base': 'j', 'letters': /[\u0135]/g },
    { 'base': 'k', 'letters': /[\u0137\u0138]/g },
    { 'base': 'l', 'letters': /[\u013A\u013C\u013E\u0140\u0142]/g },
    { 'base': 'n', 'letters': /[\u00F1\u0144\u0146\u0148\u0149\u014B]/g },
    { 'base': 'o', 'letters': /[\u00F2\u00F3\u00F4\u00F5\u00F6\u014D\u014F\u0151\u01A1\u01D2\u01FF]/g },
    { 'base': 'oe', 'letters': /[\u0153]/g },
    { 'base': 'r', 'letters': /[\u0155\u0157\u0159]/g },
    { 'base': 's', 'letters': /[\u015B\u015D\u015F\u0161]/g },
    { 'base': 't', 'letters': /[\u0163\u0165\u0167]/g },
    { 'base': 'u', 'letters': /[\u00F9\u00FA\u00FB\u00FC\u0169\u016B\u016B\u016D\u016F\u0171\u0173\u01B0\u01D4\u01D6\u01D8\u01DA\u01DC]/g },
    { 'base': 'w', 'letters': /[\u0175]/g },
    { 'base': 'y', 'letters': /[\u00FD\u00FF\u0177]/g },
    { 'base': 'z', 'letters': /[\u017A\u017C\u017E]/g },
    { 'base': 'A', 'letters': /[\u00C1\u00C2\u00C3\uCC04\u00C5\u00E0\u0100\u0102\u0104\u01CD\u01FB]/g },
    { 'base': 'AE', 'letters': /[\u00C6]/g },
    { 'base': 'C', 'letters': /[\u00C7\u0106\u0108\u010A\u010C]/g },
    { 'base': 'D', 'letters': /[\u010E\u0110\u00D0]/g },
    { 'base': 'E', 'letters': /[\u00C8\u00C9\u00CA\u00CB\u0112\u0114\u0116\u0118\u011A]/g },
    { 'base': 'G', 'letters': /[\u011C\u011E\u0120\u0122]/g },
    { 'base': 'H', 'letters': /[\u0124\u0126]/g },
    { 'base': 'I', 'letters': /[\u00CD\u00CC\u00CE\u00CF\u0128\u012A\u012C\u012E\u0049]/g },
    { 'base': 'IJ', 'letters': /[\u0132]/g },
    { 'base': 'J', 'letters': /[\u0134]/g },
    { 'base': 'K', 'letters': /[\u0136]/g },
    { 'base': 'L', 'letters': /[\u0139\u013B\u013D\u013F\u0141]/g },
    { 'base': 'N', 'letters': /[\u00D1\u0143\u0145\u0147\u0149\u014A]/g },
    { 'base': 'O', 'letters': /[\u00D2\u00D3\u00D4\u00D5\u00D6\u014C\u014E\u0150\u01A0\u01D1]/g },
    { 'base': 'OE', 'letters': /[\u0152]/g },
    { 'base': 'R', 'letters': /[\u0154\u0156\u0158]/g },
    { 'base': 'S', 'letters': /[\u015A\u015C\u015E\u0160]/g },
    { 'base': 'T', 'letters': /[\u0162\u0164\u0166]/g },
    { 'base': 'U', 'letters': /[\u00D9\u00DA\u00DB\u00DC\u0168\u016A\u016C\u016E\u0170\u0172\u01AF\u01D3\u01D5\u01D7\u01D9\u01DB]/g },
    { 'base': 'W', 'letters': /[\u0174]/g },
    { 'base': 'Y', 'letters': /[\u0178\u0176]/g },
    { 'base': 'Z', 'letters': /[\u0179\u017B\u017D]/g },
    // Greek letters
    { 'base': 'ALPHA', 'letters': /[\u0391\u03B1]/g },
    { 'base': 'BETA', 'letters': /[\u0392\u03B2]/g },
    { 'base': 'GAMMA', 'letters': /[\u0393\u03B3]/g },
    { 'base': 'DELTA', 'letters': /[\u0394\u03B4]/g },
    { 'base': 'EPSILON', 'letters': /[\u0395\u03B5]/g },
    { 'base': 'ZETA', 'letters': /[\u0396\u03B6]/g },
    { 'base': 'ETA', 'letters': /[\u0397\u03B7]/g },
    { 'base': 'THETA', 'letters': /[\u0398\u03B8]/g },
    { 'base': 'IOTA', 'letters': /[\u0399\u03B9]/g },
    { 'base': 'KAPPA', 'letters': /[\u039A\u03BA]/g },
    { 'base': 'LAMBDA', 'letters': /[\u039B\u03BB]/g },
    { 'base': 'MU', 'letters': /[\u039C\u03BC]/g },
    { 'base': 'NU', 'letters': /[\u039D\u03BD]/g },
    { 'base': 'XI', 'letters': /[\u039E\u03BE]/g },
    { 'base': 'OMICRON', 'letters': /[\u039F\u03BF]/g },
    { 'base': 'PI', 'letters': /[\u03A0\u03C0]/g },
    { 'base': 'RHO', 'letters': /[\u03A1\u03C1]/g },
    { 'base': 'SIGMA', 'letters': /[\u03A3\u03C3]/g },
    { 'base': 'TAU', 'letters': /[\u03A4\u03C4]/g },
    { 'base': 'UPSILON', 'letters': /[\u03A5\u03C5]/g },
    { 'base': 'PHI', 'letters': /[\u03A6\u03C6]/g },
    { 'base': 'CHI', 'letters': /[\u03A7\u03C7]/g },
    { 'base': 'PSI', 'letters': /[\u03A8\u03C8]/g },
    { 'base': 'OMEGA', 'letters': /[\u03A9\u03C9]/g }


];

const removeDiacritics = function (str) {
    for (var i = 0; i < defaultDiacriticsRemovalMap.length; i++) {
        str = str.replace(defaultDiacriticsRemovalMap[i].letters, defaultDiacriticsRemovalMap[i].base);
    }
    return str;
};

const regExpEscape = function (s) {
    return String(s).replace(/([-()\[\]{}+?*.$\^|,:#<!\\])/g, '\\$1').
        replace(/\x08/g, '\\x08');
};

router.get('/organizaciones', async function (req, res) {
    const search = req.query.search;
    const expWord = removeDiacritics(regExpEscape(search).toLowerCase()) + ".*";

    const db = await getConnection();
    const Organizaciones = db.collection('organizaciones');
    const orgs = await Organizaciones.find({ nombre: { $regex: expWord, $options: 'i' } }).toArray();
    return res.json(orgs);
});

router.get('/semanticTags', async function (req, res) {
    const search = req.query.search;
    const expWord = removeDiacritics(regExpEscape(search).toLowerCase()) + ".*";

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
                                "pacientes": { $push: "$registros.paciente" }
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
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    const data = results[0];
    combine(data.profesionales, data.profesionales_primera);
    combine(data.prestacion, data.prestacion_primera);
    return res.json(results[0]);
});

function combine(listA, listB) {
    listA.forEach((item) => {
        itemB = listB.find(i => String(i._id) === String(item._id));
        item.primera = itemB ? itemB.primera : 0;
    });
}

router.post('/rup/terms', async function (req, res) {
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
                    { 'concepto.conceptId': conceptId }
                    // { 'concepto.statedAncestors': conceptId }
                ]

            }
        },
        { $unwind: '$registros' },
        { $match: postFiltros },
        {
            $group: {
                _id: '$registros.term',
                count: { $sum: 1 }
            }
        }
    ];
    const results = await PrestacionesTx.aggregate(pipeline).toArray();
    return res.json(results);
});

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