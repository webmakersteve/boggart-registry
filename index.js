'use strict';

if (process.env.NODE_ENV === 'production') {
    require('newrelic');
}

var express = require('express');
var cors = require('./lib/cors');
var bodyparser = require('body-parser');
var app = express();

if (typeof process.env.PORT === 'undefined') {
    process.env.PORT = 3000;
}

if (typeof process.env.DATABASE_URL === 'undefined') {
    process.env.DATABASE_URL = "";

}

app.configure(function () {
    app.use(cors);
    app.use(express.logger());
    app.use(express.compress());
    app.use(bodyparser.urlencoded({ extended: false }));
    app.use(bodyparser.json());
    app.use(app.router);
    app.use(express.errorHandler({
        dumpExceptions: true,
        showStack: true
    }));
});

app.listen(process.env.PORT);

exports.app = app;

require('./lib/routes');

console.log('This app is using port ' + process.env.PORT + ' and the database url is ' + process.env.DATABASE_URL + '.');

//Tests look for this string to make sure the server is loaded
console.log('ready.');
