var express = require('express');
var router = express.Router();
var MongoClient = require('mongodb').MongoClient;

var mongoConnection = process.env['MONGO_DB_CONN'] || "localhost:27017";
var databases = {};

var performMongoDbRequest = function (callback) {
    if (databases['andes']) {
        callback(databases['andes']);
    } else {
        MongoClient.connect("mongodb://" + mongoConnection + "/andes", function (err, db) {
            if (err) {
                console.warn(err.message);
                process.exit();
            }
            databases['andes'] = db;
            callback(db);
        });
    }
}

router.post('/rup', function (req, res) {
    performMongoDbRequest((db) => {
        const PrestacionesTx = db.collection('prestaciontx2');
        // const PrestacionesTx = db.collection('prestacionesTx');
        const conceptsIds = req.body.concepts;
        function makeFacet(sctid) {
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
                        // total: { $sum: 1 },
                        total: { $sum: '$total' },
                        exact: {
                            $sum: {
                                $cond: { if: { $eq: ['$concepto.conceptId', sctid] }, then: '$total', else: 0 }
                                // $cond: { if: { $eq: ['$concepto.conceptId', sctid] }, then: 1, else: 0 }
                            }
                        },
                        // children: {
                        //     $sum: {
                        //         $cond: { if: { $eq: ['$concepto.conceptId', sctid] }, then: 0, else: 1 }
                        //     }
                        // }
                    }
                }
            ];
        }

        const facets = {};
        conceptsIds.forEach(c => {
            facets[c] = makeFacet(c);
        });

        const pipeline = [
            {
                $match: {
                    $or: [
                        { 'concepto.conceptId': { $in: conceptsIds } },
                        { 'concepto.statedAncestors': { $in: conceptsIds } }
                    ]

                }
            },
            {
                $facet: facets
            }
        ];
        console.log(JSON.stringify(pipeline));
        PrestacionesTx.aggregate(pipeline, (err, results) => {
            if (err) {
                return res.status(422).json(err);
            }
            if (results) {
                const data = {};
                Object.keys(results[0]).forEach(key => {
                    if (results[0][key][0]) {
                        data[key] = results[0][key][0];
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
                res.json(data);
            } else {
                const data = {};
                conceptsIds.forEach(c => {
                    data[key] = {
                        exact: 0,
                        total: 0,
                        children: 0
                    }
                });
                res.json(data);
            }

        });


    });
});

router.post('/rup/demografia', function (req, res) {
    performMongoDbRequest((db) => {
        // const PrestacionesTx = db.collection('prestacionesTx');
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
            // {
            //     $addFields: {
            //         'paciente.edad': {
            //             $divide: [{
            //                 $subtract: [
            //                     '$fecha.validacion',
            //                     '$paciente.fechaNacimiento'
            //                 ]
            //             },
            //             (365 * 24 * 60 * 60 * 1000)]
            //         }
            //     }
            // },
            {
                $addFields: {
                    'registros.paciente.decada': { $trunc: { $divide: ['$registros.paciente.edad', 10] } }
                }
            },
            {
                $group: {
                    _id: { decada: '$registros.paciente.decada', sexo: '$registros.paciente.sexo' },
                    count: { $sum: 1 }
                }
            },
            {
                $project: {
                    _id: 0,
                    decada: '$_id.decada',
                    sexo: '$_id.sexo',
                    count: '$count'
                }
            }
        ];
        PrestacionesTx.aggregate(pipeline, (err, results) => {
            if (err) {
                res.status(422).json(err);
            }
            res.json(results);
        });


    });
});

router.get('/rup/maps', function (req, res) {
    performMongoDbRequest((db) => {
        // const PrestacionesTx = db.collection('prestacionesTx');
        const PrestacionesTx = db.collection('prestaciontx2');
        const conceptId = req.query.conceptId;

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
            {
                $lookup: {
                    from: 'pacientes',
                    localField: 'registros.paciente.id',
                    foreignField: '_id',
                    as: 'pacienteData'
                }
            },
            {
                $addFields: {
                    'direccion': { $arrayElemAt: ['$pacienteData.direccion', 0] }
                }
            },
            {
                $lookup: {
                    from: 'localidades',
                    localField: 'direccion.ubicacion.localidad.nombre',
                    foreignField: 'nombre',
                    as: 'localidad'
                }
            },
            {
                $project: {
                    localidad: 1
                }
            }

        ];
        function desvio() {
            return (Math.floor(Math.random() * 40000) - 20000) / 1000000;
        }
        PrestacionesTx.aggregate(pipeline, (err, results) => {
            if (err) {
                res.status(422).json(err);
            }
            const r = results.map(l => {
                if (l.localidad.length > 0) {
                    return l.localidad[0].location;
                } else {
                    return { "lat": -38.9516784, "lng": -68.0591888 }
                }
            }).map(point => {
                return {
                    lat: point.lat + desvio(),
                    lng: point.lng + desvio()
                }
            });
            res.json(r);
        });


    });
});

/*
-38.951929, -68.059161
-38.951739, -68.073225

1KM = 0.020000
20KM = 2

*/
module.exports = router;