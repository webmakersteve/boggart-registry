'use strict';

var redis = require('redis');
var serverStatus = require('./status');
var config = require('./config');
var urlUtil = require('url');
var _ = require('lodash');

var connectionString = config.databaseUrl;

var db = {
    get: 0,
    insert: 1,
    delete: 2,
    update: 3,
    getKeys: 4
};

db.connect = function (next) {

    var client;
    var done = function () {
        client.quit();
    };
    var resolve = function (message) {
        return next(false, message, done);
    };
    var reject = function (err) {
        return next(err, null, null);
    };

    if (connectionString) {
        var redisURL = urlUtil.parse(connectionString);

        client = redis.createClient(redisURL.port, redisURL.hostname);
        client.auth(redisURL.auth.split(':')[1], function (err) {
        // After password is sent
        });

    } else {
        client = redis.createClient();
    }

    client.on('error', function (err) {
        reject(err);
    });

    client.on('connect', function () {
        resolve(client);
    });
};

// Modes
db.setHash = function (client, key, data, callback) {

    var finished = 0;
    var total = Object.keys(data).length;

    var increment = function (err, results) {
        finished++;
        if (finished === total) {
            return callback();
        }
    };

    var str;
    for (var x in data) {
        str = data[x];

        client.hset(key, x, str, increment);
    }

};

db.query = exports.query = function (mode, query, callback) {

    if (typeof callback !== 'function') {
        callback = function () {};
    }

    var code = this;

    this.connect(function (error, client, done) {

        if (error) {
            serverStatus.errors.dbConnect++;
            done();
            return callback(error);
        }

        var key = query.key || query;
        switch (mode) {
            case code.insert:
                var data = query.data || false;
                if (!data) {
                    // We have no data to insert;
                    serverStatus.errors.dbConnect++;
                    done();
                    return callback('No supplied data', null);
                }

                client.HMSET(key, data);

                callback(false, 'Successfully entered');

                break;
            case code.delete:
                client.hget(query, function (error, result) {
                    done(); // Release the database handle
                    callback(error, result);
                });
                break;
            case code.update:
                client.hget(query, function (error, result) {
                    done(); // Release the database handle
                    callback(error, result);
                });
                break;
            case code.getKeys:

                var multi = client.multi();

                client.keys(query, function (err, results) {
                    _.forEach(results, function(reply) {
                        console.log(reply);
                        multi.hgetall(reply.toString());
                    });

                    multi.exec(function (err, replies) {
                        done(); // Release the database handle
                        callback(error, replies);
                    });

                });

                break;
            default:
                //check if we're doing keys or doing not keys

        }

    });
};

exports.getPackage = function (name, callback) {
    db.query(db.get, 'packages_' + name, callback);
};

exports.getPackages = function (callback) {
    db.query(db.getKeys, 'packages_*', callback);
};

exports.insertPackage = function (name, url, callback) {
    db.query(db.insert, {
        key: 'packages_' + name,
        data: {
            name: name,
            url: url
        }
    }, callback);
};

exports.deletePackage = function (name, callback) {
    db.query(db.delete, 'DELETE FROM packages WHERE name = $1', [name], callback);
};

exports.hit = function (name) {
    db.query(db.update, 'UPDATE packages SET hits = hits + 1 WHERE name = $1', [name]);
};

exports.searchPackages = function (term, callback) {
    //a bit more complex
    db.query(db.get, 'SELECT name, url FROM packages WHERE name ILIKE $1 OR url ILIKE $1 ORDER BY hits DESC', ['%' + term + '%'], callback);
};
