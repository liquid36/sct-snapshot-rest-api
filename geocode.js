var MongoClient = require('mongodb').MongoClient;
const mongoConnection = process.env['MONGO_DB_CONN'] || "localhost:27018";


const googleMapsClient = require('@google/maps').createClient({
    key: 'AIzaSyC__of8PZKirB_IvkjzI7XTlfYtLieGRh0'
});

async function geocode(address) {
    return new Promise((resolve, reject) => {
        googleMapsClient.geocode({ address: address }, function (err, response) {
            if (err) { return reject(err) };
            return resolve(response.json.results);
        });
    })
}

async function main() {
    const db = await MongoClient.connect("mongodb://" + mongoConnection + "/andes");
    db.createCollection('localidades');
    const pacientesDB = db.collection('pacientes');
    const localidadesDB = db.collection('localidades');

    let localidades = await pacientesDB.distinct('direccion.ubicacion.localidad.nombre');

    const prs = localidades.map(async (localidad) => {
        const response = await geocode(`${localidad}, argentina`);
        const location = response[0].geometry.location;
        console.log(location)
        return await localidadesDB.update(
            { nombre: localidad },
            {
                $setOnInsert: { nombre: localidad },
                $set: { location: location }
            },
            { upsert: true }
        );
    });
    await Promise.all(prs);
    db.close();
}

main();