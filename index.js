const os = require('os');
const path = require('path');
const mkdirp = require('mkdirp');
const router = require('express').Router();
const sqlite3 = require('sqlite3').verbose();
const uniqueToken = require('unique-token');

const utils = require('./utils');
const DEFAULT_PATH = path.join(os.homedir(), 'sqcrud/db');

/**
 * 
 * @param {{dbPath:string,dbname:string,tableName:string,fields:Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>}} config 
 */
function cruder(config) {
    if (!config.dbPath) {
        config.dbPath = DEFAULT_PATH;
    }
    if (!config.dbname) {
        throw new Error('dbname is required.');
    }
    if (!config.tableName) {
        throw new Error('tableName is required.');
    }
    if (!config.fields || config.fields.length == 0) {
        throw new Error('You must provide at least one field in fields.');
    }
    mkdirp.sync(config.dbPath);
    const db = new sqlite3.Database(path.join(config.dbPath, config.dbname + '.db'));
    db.run(`CREATE TABLE IF NOT EXISTS ${config.tableName} (${utils.createTableStatement(config.fields)})`);
    const e = {};
    e.db = db;
    e.count = () => {
        return new Promise((resolve, reject) => {
            db.get(`SELECT count(*) FROM ${config.tableName}`, function (err, row) {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    };
    e.list = () => {
        return new Promise((resolve, reject) => {
            db.all(`SELECT * FROM ${config.tableName}`, function (err, rows) {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    };
    e.get = (id) => {
        return new Promise((resolve, reject) => {
            if (!id) {
                reject({ message: 'No id provided to get record' });
                return;
            }
            db.get(`SELECT * FROM ${config.tableName} WHERE _id='${id}'`, function (err, row) {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    };
    e.put = (id, data) => {
        return new Promise((resolve, reject) => {
            const stmt = utils.updateStatement(config.fields, data);
            if (!id) {
                reject({ message: 'No id provided to update record' });
                return;
            }
            if (!stmt) {
                reject({ message: 'data has no matching field to update' });
                return;
            }
            db.run(`UPDATE ${config.tableName} ${stmt} WHERE _id='${id}'`, function (err) {
                if (err) {
                    reject(err);
                } else {
                    if (this.changes > 0) {
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                }
            });
        });
    };
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            if (!data._id) {
                data._id = uniqueToken.token();
            }
            const stmt = utils.insertStatement(config.fields, data);
            if (!stmt) {
                reject({ message: 'No data to insert' });
                return;
            }
            db.run(`INSERT INTO  ${config.tableName} ${stmt}`, function (err1) {
                if (err1) {
                    reject(err1);
                } else {
                    db.get(`SELECT * FROM ${config.tableName} WHERE rowId=${this.lastID}`, function (err2, row) {
                        if (err2) {
                            reject(err2);
                        } else {
                            resolve(row);
                        }
                    });
                }
            });
        });
    };
    e.delete = (id) => {
        return new Promise((resolve, reject) => {
            if (!id) {
                reject({ message: 'No id provided to delete record' });
                return;
            }
            db.run(`DELETE FROM ${config.tableName} WHERE _id='${id}'`, function (err) {
                if (err) {
                    reject(err);
                } else {
                    if (this.changes > 0) {
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                }
            });
        });
    };
    return e;
}


/**
 * 
 * @param {{dbPath:string,dbname:string,tableName:string,fields:Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>}} config 
 */
function express(config) {
    const crud = cruder(config);
    router.get('/', (req, res) => {
        crud.list().then(rows => {
            res.status(200).json(rows);
        }).catch(err => {
            res.status(500).json(err);
        });
    });
    router.get('/:id', (req, res) => {
        crud.get(req.params.id).then(row => {
            res.status(200).json(row);
        }).catch(err => {
            res.status(500).json(err);
        });
    });
    router.post('/', (req, res) => {
        crud.post(req.body).then(row => {
            res.status(200).json(row);
        }).catch(err => {
            res.status(500).json(err);
        });
    });
    router.put('/:id', (req, res) => {
        crud.put(req.params.id, req.body).then(status => {
            res.status(200).json({ status: status });
        }).catch(err => {
            res.status(500).json(err);
        });
    });
    router.delete('/:id', (req, res) => {
        crud.delete(req.params.id).then(status => {
            res.status(200).json({ status: status });
        }).catch(err => {
            res.status(500).json(err);
        });
    });
    return router;
}

module.exports.cruder = cruder;
module.exports.express = express;