var cluster = require('cluster');
var mongo = require('mongodb'); 
var BSON = mongo.BSONPure; 
var _  = require('cloneextend');

var config = require('./config.js');
var performer = require('./performing.js'); 
   
var clustertools = require('./modules/clusterops.js'); 
var mongodb = require('./modules/mongoops.js'); 
var fstools = require('./modules/fileops.js'); 

var logEntriesCollName = config.mongo.logEntriesCollName,
	foundFilesCollName = config.mongo.foundFilesCollName; 

var numCPUs = config.environment.CPUs; 


exports.runThroughServers = function(args, callback) {

	var farmSize = args.farmSize; 
	var logDirName = args.logDirNamePrefix; 
	var countDown = farmSize; 

	mongodb.blindInitialize(function (dbx) {

		//guarantee no duplicates for foundFiles
		dbx.collection(foundFilesCollName).ensureIndex({ s_id : 1, fn : 1}, {unique : true}, function (err, result) {
			if (err) {
				throw err; 
			} else {
				//generate index for user_id (u_id) and date(dt) for the log entry
				dbx.collection(logEntriesCollName).ensureIndex({u_id : 1, dt : 1}, {unique : false} , function (err, result) {
					if (err) {
						throw err; 
					} else {
						//generate index for rollback on workers that wasn't unable to accomplish and fulfill the designated task
						dbx.collection(logEntriesCollName).ensureIndex({f_id : 1}, {unique : false}, function (err, result) {
							if (err) {
								throw err; 
							} else { 
								for (var sn = 1; sn <= farmSize; sn++) {
									var server_id = sn; 
									var url = __dirname + '/' + logDirName + sn;
								    defineServerWorkLoad(server_id, url, dbx, function () {
								  		if (0===--countDown && callback) {
								  			callback(dbx); 
								  		}
								  	});
								};
							}
						});
					}
				});								
			}
		});
	});

	function defineServerWorkLoad(server_id, url, dbx, callback) {

		fstools.getFilesListOnServer(server_id, url, function (filesList){

			if (filesList.length) {
				dbx.collection(foundFilesCollName, function (err, collection){
					if (err) {
						console.log(err); 
					} else {
						collection.insert(filesList, function(err, docs) {
							var okCallback = false; 
							if (err) {
								//duplication caught by unique index
								//overlook it
								if (err.code == 11000) {
									console.log ("server #" + server_id + " has been previously processed for work assessment, drop foundFiles collection, remove docs with this s_id field or ignore this error"); 
									okCallback = true; 
								} else {
								  console.log('following error code has ocurred : #' + err.code)
								}
							} else {
								console.log('files successfully gathered at server #:' + server_id);
								okCallback = true; 
							}
							if (okCallback && callback) {
								callback(); 
							}
						});
					}
				});
			}
		});
	}

}

exports.runThroughFoundFilesCollectionAndGetNextFiles = function(dbx, filesNumber, callback) {

	var mainQuery = {'st' : 'available'}; 
	var fieldProjection = {'s_id' : 1, '_id' : 0}

	dbx.collection(foundFilesCollName).distinct('s_id', mainQuery, function(err, serverIds) {

		if (err) {
			console.log(err);
		} else {
			findNextLogFilesToProcess(serverIds); 
		}

	});

	function findNextLogFilesToProcess(serverIds) {

		//container for found _id's - this is needed for exclusion on the remaining server to process
		//if there is only one server with files to fetch and two CPUS available, then both CPUs ougth to work
		//on that remaining server
		var nextDocs_ids = []; 
		var nextDocs = []; 

		var serversLeftToServe = serverIds; 

		if (serverIds.length) {
			getNextAvailableFile(); 
		} else {
			callback(null);
		}

		//recursive function. Runs while condition of its first line isn't false
		function getNextAvailableFile () {

			if (nextDocs_ids.length < filesNumber) {

				var subQuery = _.clone(mainQuery); 

				if (nextDocs_ids.length) {
					//excludes the possibility of queueing duplicates
					subQuery._id = {$nin : nextDocs_ids};
				}

				if (serversLeftToServe.length) {
					//gets the first available server on the list 
					subQuery.s_id = serversLeftToServe[0];
				}

				dbx.collection(foundFilesCollName).findOne(subQuery, function (err, doc) {

					if (err) { 						
						console.log('Error on finding one available file: \n');
						console.log(err);
					} else {
						if (doc !== null) {

							nextDocs_ids.push(doc._id);
							nextDocs.push(doc); 

							//remove first element from serversLeftToServe 
							if (serversLeftToServe.length > (filesNumber - nextDocs_ids.length)) {
								serversLeftToServe.shift(); 							
							}

							if (nextDocs.length == filesNumber) {
								callback(nextDocs); 
							} else {
								getNextAvailableFile(); 
							}		
						} else {
							//no more files available
							if (callback) callback(nextDocs); 
						}
					}
				});
			}
		}
	}
}


exports.prepareAndAssignWork  = function(dbx, docsToProcess, callback) {

	var countDown = docsToProcess.length;

	docsToProcess.forEach(function (doc, index) {

		updateFoundFileStatus(dbx, doc, index + 1, function (done, workerId) {

			if (done) {

				clustertools.initializeWorker(true, doc._id, workerId, function (forked) {				

					if (forked) {

						if (0===--countDown && callback) callback(true); 

					}
				}); 
			}
		}); 
	});

	function updateFoundFileStatus (dbx, doc, workerId, callback) {
		var query = {
			_id : doc._id
		};

		var modifying = {
			$set : {
				st 		: 'on',
				a_wk	: workerId
			}
		}; 
		dbx.collection(foundFilesCollName).update(query, modifying, function (err, updated){
			var done = false; 
			if (err) {
				console.log('updating foundFiles collections for assignment work has broken out a complete mayhem. Debug this and find what is wrong');
			} else {
				console.log(doc.fn + ' foundFile doc has been updated and assigned towards worker # ' + workerId);
				done = true; 
			}
			if (callback) callback(done, workerId); 
		})
	}
}