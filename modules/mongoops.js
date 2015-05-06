var mongo = require('mongodb'),
    mongoClient = mongo.MongoClient, 
    Server = mongo.Server,
    Db = mongo.Db,
    BSON = mongo.BSONPure,
    db;

var config = require('../config.js');

exports.blindInitialize = function (callback) {
    mongoClient.connect(config.mongo.connectionString, function(err, dbx) {
        if (err) {
            console.log(err); 
        } else {
            db = dbx; 
            if (callback) {
                callback (dbx); 
            }
        }
    });
}

var closeDb = function () {
    db.close(function (err, result) {
        if (err) {
          console.log(err); 
        } else {
          console.log(result); 
        }

    });
}

