
/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 */
function createTableStatement(fields) {
    const temp = [];
    fields.forEach(field => {
        let str = '';
        str += field.key + ' ' + field.type;
        if (field.primaryKey) {
            str += ' PRIMARY KEY'
        } else if (field.unique) {
            str += ' UNIQUE'
        }
        if (field.required) {
            str += ' NOT NULL'
        }
        temp.push(str);
    });
    return temp.join(', ');
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {any} data
 */
function insertStatement(fields, data) {
    const cols = [];
    const values = [];
    Object.keys(data).forEach(dataKey => {
        const temp = fields.find(e => e.key === dataKey);
        if (temp) {
            cols.push(temp.key);
            if (temp.type === 'TEXT' || temp.type === 'BLOB') {
                values.push(`'${escape(data[temp.key])}'`);
            } else {
                values.push(data[temp.key]);
            }
        }
    });
    if (values.length > 1) {
        return `(${cols.join(', ')}) VALUES(${values.join(', ')})`;
    }
    return null;
}

/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {any} data
 */
function updateStatement(fields, data) {
    const sets = [];
    Object.keys(data).forEach(dataKey => {
        const temp = fields.find(e => e.key === dataKey);
        if (temp && dataKey !== '_id') {
            if (temp.type === 'TEXT' || temp.type === 'BLOB') {
                sets.push(`${temp.key}='${escape(data[temp.key])}'`);
            } else {
                sets.push(`${temp.key}=${data[temp.key]}`);
            }
        }
    });
    if (sets.length > 0) {
        return 'SET ' + sets.join(', ');
    }
    return null;
}

module.exports.createTableStatement = createTableStatement;
module.exports.insertStatement = insertStatement;
module.exports.updateStatement = updateStatement;