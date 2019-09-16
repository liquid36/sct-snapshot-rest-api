const { FILTER_AVAILABLE, TIME_UNIT, getFilterField } = require('../util');
const base64 = require('base-64');

function selfOrDescendant(conceptId, type, self) {
    const $or = [
        { 'concepto.conceptId': conceptId },
    ]
    if (!self) {
        if (type === 'stated') {
            $or.push({ 'concepto.statedAncestors': conceptId });
        } else {
            $or.push({ 'concepto.inferredAncestors': conceptId });
        }
    }
    return $or;
}

function initialMatch(conceptId, type, periodo, self) {
    if (periodo.end) {
        return {
            $or: selfOrDescendant(conceptId, type, self),
            start: periodo.start.clone().startOf(TIME_UNIT).toDate()
        };
    } else {
        return {
            $or: selfOrDescendant(conceptId, type, self),
            start: periodo.start.toDate()
        };
    }
}

function makeBasePipeline(concept, periodo, params, options = {}) {
    const { forceUnwind, self } = options;
    const $match = initialMatch(concept, params.type, periodo, self);
    const $unwindMatch = {}
    const extrasStage = [];
    FILTER_AVAILABLE.forEach(filter => {
        const name = filter.name;
        if (params[name] && filter.field) {
            $match[filter.field] = params[name];
            if (filter.unwind) {
                $unwindMatch[filter.field] = params[name];
            }
        }
    });


    const needUnwind = Object.keys($unwindMatch).length > 0 || forceUnwind || periodo.end;
    if (needUnwind) {
        if (periodo.end) {
            $unwindMatch['registros.fecha'] = { $gte: periodo.start.toDate(), $lte: periodo.end.toDate() };
        }
        extrasStage.push({ $unwind: '$registros' });
        extrasStage.push({ $match: $unwindMatch });
    }

    return {
        pipeline: [
            { $match: $match },
            ...extrasStage,
        ],
        needUnwind
    };
}

function addOnQueryEdad(metadata, params) {
    const bucketField = `docs.${metadata.field}`;
    const $addFields = {};
    $addFields[bucketField] = '$_id';

    return [
        {
            $bucket: {
                groupBy: `$${metadata.field}`,
                boundaries: params.rangoEtario,
                default: 100,
                output: {
                    "docs": { $push: "$$ROOT" },
                }
            }
        },
        { $unwind: '$docs' },
        { $addFields },
        { $replaceRoot: { newRoot: '$docs' } }
    ]
}


function createIdMetadata(groups) {
    if (groups) {
        const label = {}
        const _id = {};
        groups.forEach(groupBy => {
            const metadata = getFilterField(groupBy);
            _id[groupBy] = '$' + metadata.field;
            label['label_' + groupBy] = { $first: '$' + metadata.label };
        });
        return {
            _id, ...label
        };
    } else {
        return {
            _id: ''
        };
    }
}

function createLabelMetadata(groups) {
    if (groups) {
        const label = {}
        groups.forEach(groupBy => {
            label[groupBy] = '$label_' + groupBy;
        });
        return label;
    } else {
        return 1;
    }
}

const addOnField = {
    'edad': addOnQueryEdad
}

function createAddOn(groups, params) {
    let extras = [];
    groups.forEach(groupBy => {
        if (addOnField[groupBy]) {
            const metadata = getFilterField(groupBy);
            const addons = addOnField[groupBy](metadata, params);
            extras = [...extras, ...addons];
        }
    });
    return extras;
}

function hash(id) {
    return base64.encode(JSON.stringify(id));
}

function groupReducer(metadata) {
    return (acc, value) => {
        value.forEach(e => {
            const e_acc = acc.find(i => i.hashId === e.hashId);
            if (e_acc) {
                e_acc.value = metadata.reducer(e_acc.value, e.value);
            } else {
                acc.push(e);
            }
        });
        return acc;
    }
}

module.exports.hash = hash;
module.exports.groupReducer = groupReducer;
module.exports.makeBasePipeline = makeBasePipeline;
module.exports.createAddOn = createAddOn;
module.exports.createIdMetadata = createIdMetadata;
module.exports.createLabelMetadata = createLabelMetadata;