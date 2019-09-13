const moment = require('moment');

const { getConnection, MAIN_DB } = require('./database');
const { TIME_UNIT, FILTER_AVAILABLE } = require('./util');
const { storeInCache, restoreFromCache, touchCache } = require('./cache');


let date_min = moment('2018-01-01T00:00:00.000-03:00').startOf(TIME_UNIT);
let date_max = moment('2019-08-31T00:00:00.000-03:00').endOf(TIME_UNIT);

async function minmaxDate() {
    const db = await getConnection();
    const list = await db.collection(MAIN_DB).aggregate([{ $group: { _id: null, max: { $max: '$start' }, min: { $min: "$start" } } }]).toArray();
    date_max = moment(list[0].max).endOf(TIME_UNIT);
    date_min = moment(list[0].min).startOf(TIME_UNIT);
}

minmaxDate();

module.exports.execQuery = async function (name, conceptsIds, filters) {
    let cache = {};
    const queryData = require('./queries/' + name);

    if (!queryData) {
        throw new Error('Visuzlization not found!');
    }
    const { start, end, params } = parseFilter(filters);
    const periods = splitTimeline(start, end);
    if (queryData.cache) {
        cache = await restoreFromCache(name, conceptsIds, start, end, params);
    }

    const results = {};
    const ps = conceptsIds.map(async concept => {
        results[concept] = await execQueryByConcept(name, queryData, concept, periods, cache[concept] || [], params);
    });
    await Promise.all(ps);
    return results;
}

async function execQueryByConcept(name, queryData, conceptId, periodos, cache, params) {
    const ps = periodos.map(async periodo => {
        if (periodo.end) {
            if (!queryData.query) {
                throw new Error(`Visualization [${name}] not have longQuery function`);
            }
            return await queryData.query(conceptId, periodo, params);
        } else {
            const inCache = cache.find(c => periodo.start.isSame(c.start));
            if (inCache) {
                if (queryData.cache) {
                    touchCache(inCache);
                }
                return inCache.value;
            } else {
                if (!queryData.query) {
                    throw new Error(`Visualization [${name}] not have longQuery function`);
                }
                const qs = await queryData.query(conceptId, periodo, params);
                if (queryData.cache) {
                    storeInCache(name, conceptId, periodo.start, params, qs);
                }
                return qs;
            }
        }
    });

    const resultList = await Promise.all(ps);
    const result = resultList.reduce(queryData.reducer, queryData.initial());
    if (queryData.transform) {
        return queryData.transform(result);
    }
    return result;
}

function getDate(date, type = 'start') {
    if (date) {
        return moment(date);
    } else {
        return type === 'start' ? date_min : date_max;
    }
}

function isStartOf(date, unit = TIME_UNIT) {
    return date.clone().startOf('day').isSame(date.clone().startOf(unit));
}

function isEndOf(date, unit = TIME_UNIT) {
    return date.clone().endOf('day').isSame(date.clone().endOf(unit));
}

function isSamePeriod(start, end, unit = TIME_UNIT) {
    return start.clone().startOf(unit).isSame(end.clone().startOf(unit));
}

function parseFilter(filter) {
    const start = getDate(filter.start, 'start');
    const end = getDate(filter.end, 'end');
    let params = {
    };
    FILTER_AVAILABLE.forEach((t) => {
        const name = t.name;
        const defaultValue = t.default;
        const transform = t.transform || ((v) => v);

        if (filter[name] === undefined) {
            params[name] = defaultValue;
        } else {
            params[name] = transform(filter[name]);
        }
    });
    return { start, end, params };
}

function splitTimeline(start, end) {
    const _isStartOf = isStartOf(start);
    const _isEndOf = isEndOf(end);
    const _isSamePeriod = isSamePeriod(start, end);
    const periods = [];

    if (_isSamePeriod) {
        if (_isStartOf && _isEndOf) {
            periods.push({ start });
        } else {
            periods.push({ start, end });
        }
    } else {
        periods.push({ start, end: _isStartOf ? null : start.clone().endOf(TIME_UNIT) });
        if (_isEndOf) {
            periods.push({ start: end.clone().startOf(TIME_UNIT) });
        } else {
            periods.push({ start: end.clone().startOf(TIME_UNIT), end });

        }

        let step = start.clone().add(1, TIME_UNIT).startOf(TIME_UNIT);

        while (!isSamePeriod(step, end)) {
            periods.push({ start: step });
            step = step.clone().add(1, TIME_UNIT).startOf(TIME_UNIT);
        }
    }

    return periods;
}



/**
 * GET max and min date
 db.getCollection('prestaciontx2').aggregate([
     {
         $group:
         {
             _id: null,
             max: { $max: '$start' },
             min: { $min: "$start" }
         }
     }
   ])
 */

