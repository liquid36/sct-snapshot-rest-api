const moment = require('moment');

const { getConnection, MAIN_DB } = require('./database');
const { TIME_UNIT, FILTER_AVAILABLE } = require('./util');
const { storeInCache, restoreFromCache, touchCache } = require('./cache');
const { groupReducer } = require('./queries/helpers');

let date_min = moment('2018-01-01T00:00:00.000-03:00').startOf(TIME_UNIT);
let date_max = moment('2019-08-31T00:00:00.000-03:00').endOf(TIME_UNIT);

async function minmaxDate() {
    const db = await getConnection();
    const list = await db.collection(MAIN_DB).aggregate([{ $group: { _id: null, max: { $max: '$start' }, min: { $min: "$start" } } }]).toArray();
    date_max = moment(list[0].max).endOf(TIME_UNIT);
    date_min = moment(list[0].min).startOf(TIME_UNIT);
}

minmaxDate();

module.exports.execQuery = async function (name, conceptsIds, filters, group) {
    let cache = {};
    const queryData = require('./queries/' + name);
    const cacheActive = queryData.cache && !group;
    if (!queryData) {
        throw new Error('Visuzlization not found!');
    }
    const { start, end, params } = parseFilter(filters);
    const periods = splitTimeline(start, end);

    if (cacheActive) {
        cache = await restoreFromCache(name, conceptsIds, start, end, params);
    }

    const results = {};
    const ps = conceptsIds.map(async conceptId => {
        const self = conceptId.startsWith('!');
        const concept = self ? conceptId.substr(1) : conceptId;
        results[concept] = await execQueryByConcept(queryData, conceptId, periods, cache[concept] || [], params, group);
    });

    await Promise.all(ps);
    return results;
}

async function execQueryByConcept(queryData, conceptId, periodos, cache, params, group) {
    const cacheActive = queryData.cache && !group;
    const ps = periodos.map(async periodo => {
        if (periodo.end) {
            if (!queryData.query) {
                throw new Error(`Visualization [${queryData.name}] not have longQuery function`);
            }
            return await queryData.query(conceptId, periodo, params, group);
        } else {
            const inCache = cache.find(c => periodo.start.isSame(c.start));
            if (inCache) {
                if (cacheActive) {
                    touchCache(inCache);
                }
                return inCache.value;
            } else {
                if (!queryData.query) {
                    throw new Error(`Visualization [${queryData.name}] not have longQuery function`);
                }

                const qs = await queryData.query(conceptId, periodo, params, group);
                if (cacheActive) {
                    storeInCache(queryData.name, conceptId, periodo.start, params, qs);
                }
                return qs;
            }
        }
    });

    const resultList = await Promise.all(ps);
    let result = resultList.reduce(groupReducer(queryData), []);
    if (queryData.transform) {
        result = result.map(item => {
            return {
                _id: item._id,
                label: item.label,
                hashId: item.hashId,
                value: queryData.transform(item.value)
            }
        })
    }
    if (!group) {
        if (result.length > 0) {
            return result[0];
        } else {
            return { _id: '', hashId: '', label: '', value: queryData.transform(queryData.initial()) };
        }
    }
    return result;
}




function getDate(date, type = 'start') {
    if (date) {
        return type === 'start' ? moment(date).startOf('day') : moment(date).endOf('day');
    } else {
        return type === 'start' ? date_min.clone() : date_max.clone();
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

        if (filter[name] === undefined || filter[name] === null) {
            params[name] = defaultValue;
        } else {
            params[name] = transform(filter[name]);
        }
    });
    if (filter.rangoEtario) {
        params.rangoEtario = filter.rangoEtario;
    }
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

