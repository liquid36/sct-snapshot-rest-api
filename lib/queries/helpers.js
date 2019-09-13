const { FILTER_AVAILABLE } = require('../util');

function selfOrDescendant(conceptId, type) {
    const $or = [
        { 'concepto.conceptId': conceptId },
    ]
    if (type === 'stated') {
        $or.push({ 'concepto.statedAncestors': conceptId });
    } else {
        $or.push({ 'concepto.inferredAncestors': conceptId });
    }
    return $or;
}

function initialMatch(conceptId, type, periodo) {
    if (periodo.end) {
        return {
            $or: selfOrDescendant(conceptId, type),
            start: { $lte: periodo.start.toDate() },
            end: { $gt: periodo.end.toDate() }
        };
    } else {
        return {
            $or: selfOrDescendant(conceptId, type),
            start: periodo.start.toDate()
        };
    }
}

function makeBasePipeline(concept, periodo, params, options = {}) {
    const { forceUnwind } = options;
    const $match = initialMatch(concept, params.type, periodo);
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
            $unwindMatch['registros.fecha'] = { $gte: start.toDate(), $lte: end.toDate() };
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

module.exports.makeBasePipeline = makeBasePipeline;