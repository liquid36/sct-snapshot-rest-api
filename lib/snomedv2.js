/**
 * Created by tbertonatti on 11/1/16.
 */
const MongoClient = require('mongodb').MongoClient;
const databases = {};
const defaultTermTypes = {};

const mongoConnection = process.env['MONGO_DB_CONN'] || "localhost:27017";

const performMongoDbRequest = async function (databaseName) {
    if (databases[databaseName]) {
        return databases[databaseName];
    } else {
        try {
            const db = await MongoClient.connect(mongoConnection);
            databases[databaseName] = db;
            return db;
        } catch (err) {
            console.warn(err.message);
            process.exit();
        }
    }
};
const getObject = async function (dbP, collectionP, query, options) {
    const db = await performMongoDbRequest(dbP);
    var collection = db.collection(collectionP);

    const docs = await collection.find(query, options).toArray();
    return docs;
};

const getConcept = async function (dbP, collectionP, conceptId, options) {
    const docs = await getObject(dbP, collectionP, { 'conceptId': conceptId }, options);
    return docs[0];
};

const getDescriptions = async function (dbP, collectionP, conceptId, descriptionId, options) {
    const doc = await getConcept(dbP, collectionP, conceptId, options);
    var result = [];
    if (doc.descriptions) {
        doc.descriptions.forEach(function (desc) {
            if (descriptionId) {
                if (descriptionId == desc.descriptionId) {
                    result.push(desc);
                }
            } else {
                result.push(desc);
            }
        });
    }
    return result;
};

const getRelationShips = async function (dbP, collectionP, conceptId, form, options) {
    const doc = await getConcept(dbP, collectionP, conceptId, options);
    const result = [];
    if (doc.relationships) {
        doc.relationships.forEach(desc => {
            if (form == "all") {
                result.push(desc);
            } else if (form == "inferred" && desc.characteristicType.conceptId == "900000000000011006") {
                result.push(desc);
            } else if (form == "stated" && desc.characteristicType.conceptId == "900000000000010007") {
                result.push(desc);
            } else if (form == "additional" && desc.characteristicType.conceptId == "900000000000227009") {
                result.push(desc);
            }
        });
    }
    return result;
};

const getParents = async function (dbP, collectionP, conceptId, form, options) {
    const doc = await getConcept(dbP, collectionP, conceptId, options);
    const result = [];

    if (typeof doc.relationships != 'undefined') {
        if (form) {
            if (form == "inferred" && doc.relationships) {
                doc.relationships.forEach(function (rel) {
                    if (rel.characteristicType.conceptId == "900000000000011006" && rel.active == true && rel.type.conceptId == "116680003") {
                        result.push({
                            conceptId: rel.destination.conceptId,
                            preferredTerm: rel.destination.preferredTerm,
                            fullySpecifiedName: rel.destination.fullySpecifiedName,
                            definitionStatus: rel.destination.definitionStatus,
                            module: rel.destination.module,
                            statedDescendants: rel.destination.statedDescendants,
                            inferredDescendants: rel.destination.inferredDescendants
                        });
                    }
                });
            } else if (form == "stated" && doc.relationships) {
                doc.relationships.forEach(function (rel) {
                    if (rel.characteristicType.conceptId == "900000000000010007" && rel.active == true && rel.type.conceptId == "116680003") {
                        result.push({
                            conceptId: rel.destination.conceptId,
                            preferredTerm: rel.destination.preferredTerm,
                            fullySpecifiedName: rel.destination.fullySpecifiedName,
                            definitionStatus: rel.destination.definitionStatus,
                            module: rel.destination.module,
                            statedDescendants: rel.destination.statedDescendants,
                            inferredDescendants: rel.destination.inferredDescendants
                        });
                    }
                });
            }
        } else if (doc.relationships) {
            doc.relationships.forEach(function (rel) {
                if (rel.characteristicType.conceptId == "900000000000011006" && rel.active == true && rel.type.conceptId == "116680003") {
                    result.push({
                        conceptId: rel.destination.conceptId,
                        preferredTerm: rel.destination.preferredTerm,
                        fullySpecifiedName: rel.destination.fullySpecifiedName,
                        definitionStatus: rel.destination.definitionStatus,
                        module: rel.destination.module,
                        statedDescendants: rel.destination.statedDescendants,
                        inferredDescendants: rel.destination.inferredDescendants
                    });
                }
            });
        }
    }
    return result;
};

const getMembers = async function (dbP, collectionP, conceptId, options) {

    var query = { "memberships": { "$elemMatch": { "refset.conceptId": conceptId, "active": true } } };
    if (options.filter) {
        var searchTerm = "\\b" + regExpEscape(options.filter).toLowerCase();
        query.preferredTerm = { "$regex": searchTerm, "$options": "i" };
    }
    if (options.activeOnly == "true") {
        query.active = "true";
    }
    const getTotalOf = async function (refsetId) {
        const docs = await getObject("server", "resources", { "databaseName": dbP, "collectionName": collectionP.replace("v", "") }, { refsets: 1 });
        const total = 0;
        const error = "No refset matching in the manifest";
        docs[0].refsets.forEach(refset => {
            if (refset.conceptId == refsetId) {
                error = false;
                total = refset.count;
            }
        });
        return total;

    };
    const totalR = await getTotalOf(conceptId);
    const total = totalR;
    const docs = await getObject(dbP, collectionP, query, options);
    const result = {};
    result.members = [];
    result.details = { 'total': total, 'refsetId': conceptId };
    if (docs && docs.length > 0) {
        result.members = docs;
    }
    return result;
};

const searchDescription = async function (dbP, collectionP, filters, query, options) {
    const processMatches = function (docs) {
        var result = {};
        result.matches = [];
        result.details = { 'total': 0, 'skipTo': filters.skipTo, 'returnLimit': filters.returnLimit };
        result.filters = {};
        result.filters.lang = {};
        result.filters.semTag = {};
        result.filters.module = {};
        result.filters.refsetId = {};
        if (docs && docs.length > 0) {

            result.details = { 'total': docs.length, 'skipTo': filters.skipTo, 'returnLimit': filters.returnLimit };
            if (filters.idParamStr == docs[0].descriptionId) {
                result.matches.push({
                    "term": docs[0].term,
                    "conceptId": docs[0].conceptId,
                    "active": docs[0].active,
                    "conceptActive": docs[0].conceptActive,
                    "fsn": docs[0].fsn,
                    "module": docs[0].stringModule
                });
                return result;
            } else {
                var matchedDescriptions = docs.slice(0);
                if (filters.searchMode == "regex" || filters.searchMode == "partialMatching") {
                    matchedDescriptions.sort(function (a, b) {
                        if (a.term.length < b.term.length)
                            return -1;
                        if (a.term.length > b.term.length)
                            return 1;
                        return 0;
                    });
                } else {
                    matchedDescriptions.sort(function (a, b) {
                        if (a.score > b.score)
                            return -1;
                        if (a.score < b.score)
                            return 1;
                        return 0;
                    });
                }
                var count = 0;
                var conceptIds = [];
                matchedDescriptions.forEach(function (doc) {
                    var refsetOk = false;
                    if (doc.refsetIds) {
                        doc.refsetIds.forEach(function (refset) {
                            if (refset == filters.refsetFilter) {
                                refsetOk = true;
                            }
                        });
                    }
                    if (filters.semanticFilter == "none" || (filters.semanticFilter == doc.semanticTag)) {
                        if (filters.langFilter == "none" || (filters.langFilter == doc.languageCode)) {
                            if (filters.moduleFilter == "none" || (filters.moduleFilter == doc.stringModule)) {
                                if (filters.refsetFilter == "none" || refsetOk) {
                                    if (!filters["groupByConcept"] || conceptIds.indexOf(doc.conceptId) == -1) {
                                        conceptIds.push(doc.conceptId);

                                        if (count >= filters.skipTo && count < (filters.skipTo + filters.returnLimit)) {
                                            result.matches.push({
                                                "term": doc.term,
                                                "conceptId": doc.conceptId,
                                                "active": doc.active,
                                                "conceptActive": doc.conceptActive,
                                                "fsn": doc.fsn,
                                                "module": doc.stringModule,
                                                "definitionStatus": doc.definitionStatus
                                            });
                                        }
                                        if (result.filters.semTag.hasOwnProperty(doc.semanticTag)) {
                                            result.filters.semTag[doc.semanticTag] = result.filters.semTag[doc.semanticTag] + 1;
                                        } else {
                                            result.filters.semTag[doc.semanticTag] = 1;
                                        }
                                        if (result.filters.lang.hasOwnProperty(doc.languageCode)) {
                                            result.filters.lang[doc.languageCode] = result.filters.lang[doc.languageCode] + 1;
                                        } else {
                                            result.filters.lang[doc.languageCode] = 1;
                                        }
                                        if (result.filters.module.hasOwnProperty(doc.stringModule)) {
                                            result.filters.module[doc.stringModule] = result.filters.module[doc.stringModule] + 1;
                                        } else {
                                            result.filters.module[doc.stringModule] = 1;
                                        }
                                        if (doc.refsetIds) {
                                            doc.refsetIds.forEach(function (refset) {
                                                if (result.filters.refsetId.hasOwnProperty(refset)) {
                                                    result.filters.refsetId[refset] = result.filters.refsetId[refset] + 1;
                                                } else {
                                                    result.filters.refsetId[refset] = 1;
                                                }
                                            });
                                        }
                                        count = count + 1;
                                    }
                                }
                            }
                        }
                    }
                });
                result.details.total = count;
                return result;

            }
        } else {
            result.matches = [];
            result.details = { 'total': 0, 'skipTo': filters.skipTo, 'returnLimit': filters.returnLimit };
            return result;
        }
    };
    if (filters.searchMode == "regex" || filters.searchMode == "partialMatching") {
        const docs = await getObject(dbP, collectionP + "tx", query, options);
        return processMatches(docs);
    } else {
        const docs = await getObject(dbP, collectionP + "tx", query, { score: { $meta: "textScore" } });
        return processMatches(docs);
    }
};

const getDefaultTermType = async function (dbP, collectionP) {
    let typeId = "900000000000003001";
    if (dbP == "en-edition") {
        return typeId;
    } else if (defaultTermTypes[dbP + "-" + collectionP]) {
        return defaultTermTypes[dbP + "-" + collectionP];
    } else {
        const options = {};
        options["fields"] = { "defaultTermType": 1 };
        const query = { databaseName: dbP, collectionName: collectionP.replace("v", "") };
        const docs = await getObject("server", "resources", query, options);
        if (docs && docs.length > 0) {
            typeId = docs[0].defaultTermType;
        }
        defaultTermTypes[dbP + "-" + collectionP] = typeId;
        return typeId;
    }
};

module.exports.getObject = getObject;
module.exports.getConcept = getConcept;
module.exports.getDescriptions = getDescriptions;
module.exports.getRelationShips = getRelationShips;
module.exports.getParents = getParents;
module.exports.getMembers = getMembers;
module.exports.searchDescription = searchDescription;
module.exports.getDefaultTermType = getDefaultTermType;

var regExpEscape = function (s) {
    return String(s).replace(/([-()\[\]{}+?*.$\^|,:#<!\\])/g, '\\$1').
        replace(/\x08/g, '\\x08');
};