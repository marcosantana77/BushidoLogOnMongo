var cluster = require('cluster');
var mongo = require('mongodb'); 
var BSON = mongo.BSONPure; 
var _  = require('cloneextend');

var config = require('./config.js');
var assessor = require('./assessment.js'); 
var performer = require('./performing.js'); 
   
var clustertools = require('./modules/clusterops.js'); 
var mongodb = require('./modules/mongoops.js'); 
var fstools = require('./modules/fileops.js'); 

var logEntriesCollName = config.mongo.logEntriesCollName,
	foundFilesCollName = config.mongo.foundFilesCollName; 

var numCPUs = config.environment.CPUs; 

if (cluster.isMaster) {

	var db; 

    assessor.runThroughServers(config.runningArgs, function (dbx) {   

    	//numCPUs is sent here because this function will also be used to reassignment 
    	assessor.runThroughFoundFilesCollectionAndGetNextFiles (dbx, numCPUs, function (nextDocs) {

    		if (nextDocs === null) {
    			console.log('all files have been processed'); 
    			process.exit(); 
    		}

    		assessor.prepareAndAssignWork(dbx, nextDocs, function (jobDone) {  

    			if (jobDone) {
    				console.log("workers have been set to do the reading job"); 
    			} else {
    				console.log("well, things aren't ready just yet, keep on writing code. better results lie ahead");
    			}
			    dbx.close(); 
    		});

    		console.log('First docs to process:');
    		console.log(nextDocs); 
    	});

    	db = dbx; 
    }); 

	cluster.on('online', function(worker) {
		console.log('worker online + ' + worker.id); 

		var prop = (worker.isAssessment ? 'doc_id' : 'user_id')

	   	console.log(worker[prop]);

	   	var workerMsg = {
	   		status 				: 'on',
	   		type 				: (worker.isAssessment ? 'assessor' : 'performer'),
	   		doc_id 				: worker.doc_id,
	   		user_id				: worker.user_id, 
	   		assignedWorkerId 	: worker.assignedWorkerId,
	   		isAssessment		: worker.isAssessment	   		
	   	};

	   	worker.send(workerMsg); 
	});

	cluster.on('disconnect', function(worker) {

		var prop = (worker.isAssessment ? 'doc_id' : 'user_id')

		console.log(worker[prop] + ' worker of type '+ (worker.isAssessment ? 'assessor' : 'performer') + ' has been disconnected'); 

		//in here there is space to rollback whatven went wrong.
		//at assessment level, rollback from status:on to status:available and resetting the worker to reperform the task on same file
		//at performing level, rollback user being processed and redo processing of saving info for that user

		worker.kill(); 

	});

	cluster.on('exit', function(worker, code, signal) {
  		if (worker.suicide === true) {
    		console.log('Worker #' + worker.id + ' of type ' + (worker.isAssessment ? 'assessor' : 'performer') +  ' has committed harakiri, its job has been honorably completed!');
    		console.log('***************************************************************************************************************************************');

    		if (worker.isAssessment) {
		    	assessor.runThroughFoundFilesCollectionAndGetNextFiles (db, 1, function (nextDocs) {

		    		if ((nextDocs === null) && isEmpty(cluster.workers)) {

		    			console.log('first level of task has been completed all log data has been assessed for further tranformation'); 
						
						// **********************************
		    			// get the rest of the job done: sort users log and StreamWrite new log files
		    			// ***********************************
		    			console.log('\n**********************************************************************');
		    			console.log('**********************************************************************');
		    			console.log('**************** INITIALIZING WORK PHASE TWO *************************'); 
		    			console.log('**********************************************************************');
		    			console.log('**********************************************************************\n');
		    			performer.perform(db, function (jobDone) {

		    				console.log('return of cluster.on.listener after definition and assignment of first loaders wave. Done? ' + jobDone ); 

		    				if (!jobDone) {
		    					console.log('loading services weren\'t stablished. this might be due to lack of unprocessed users, logEntries data or exception at this stage.'); 
		    					process.exit(); 
		    				} 

		    			});

		    		} else if (nextDocs !== null) {
			    		assessor.prepareAndAssignWork(db, nextDocs, function (jobDone) {

			    			if (jobDone) {
			    				console.log("workers have been set to do the job"); 
			    			} else {
			    				console.log("well, things aren't ready just yet, keep on writing code. better results lie ahead");
			    			}
			    		});
		    		}
				});   		
			} else {

				performer.runThroughUsersListAndGetNexAvailableUser(1, function (list) {

					if (list === null) {
	 					if (isEmpty(cluster.workers)) { 

							console.log('all tasks have been completed both at phase one (assesment) and phase two (performing load)!!!'); 
							process.exit(); 

						} 
					} else {
						if (list.length > 0) {
							// performer.getDataFromLogEntriesCollectionAndSaveFile(list[0].user_id, function (jobDone) { 
							clustertools.initializeWorker(false, list[0].user_id, worker.id + 9, function (jobDone) { 
								if (jobDone) {
									console.log('job done for user ' + list[0].user_id + ' at exit worker event');
								} else {
									console.log('error at worker level for user' + list[0].user_id); 
									process.exit(); 
								}

							});
						}
					}
					 //else if !list && workers not empty, do nothing, process exit will be handled by the last remaining worker
						
				});

			}
		}
	});

} else {

	var db; 

	process.on('message', function (msg) {
		if (msg.status === 'on' && msg.type === 'assessor') {
			receivesWorkAndGetItOnWithIt(msg, function (foundFileDoc) {
				if (foundFileDoc != null) {
					extractAndTransformFile(foundFileDoc, function (err, result) {
						console.log('all good'); 
					});
				}
			});
		} else if (msg.status === 'on' && msg.type === 'performer') {
			console.log('performer work has begun at following worker: ');
			console.log(msg); 

			var user_id = msg.user_id; 

			performer.getDataFromLogEntriesCollectionAndSaveFile(user_id, function (jobDone) { 
				if (jobDone) {
					console.log('job done for user ' + user_id);
				} else {
					console.log('error at worker level for user' + user_id); 
				}
				cluster.worker.disconnect();  
			});


		}
	});

	function extractAndTransformFile(foundFileDoc, callback) {

		var bsonId = new BSON.ObjectID(foundFileDoc._id);

		fstools.readLogFileWStream(bsonId, foundFileDoc.fn, function (err, logEntries) {

			if (err) {
				console.log('following error ocurred: ' + err); 
				cluster.worker.disconnect();  
			} else {
				var dbx = db; 

				dbx.collection(logEntriesCollName).insert(logEntries, function (err, docs) {

					if (err) {

						console.log('inserting log entries from file: ' + foundFileDoc.fn + ' has gone bezerk. following error has been unleashed:');
						console.log(err);
					} else {
						console.log('inserting log entries from file: ' + foundFileDoc.fn + ' has gone smoothly succedeed');

						dbx.collection(foundFilesCollName).update({_id : bsonId}, {$set : {st : 'done'}}, function (err, result) {
							if(err) {

								console.log('ERROR: status change for file : ' + foundFileDoc.fn + ' has failed miserably with the error code: ' + err.code); 

							} else {
								console.log('status for file: ' + foundFileDoc.fn + ' has been changed to reflect completion of the work ');

								//harakiri this worker
								cluster.worker.disconnect();  
							}
						});
					}
				});
			}
		});
	}

	function receivesWorkAndGetItOnWithIt (work, callback) {

		console.log('work has started on ')

		//work.type = 'done'; 

		var updateFoundFileAssignedWorkerId = (work.a_wd != cluster.worker.id); 

		var bsonId = new BSON.ObjectID(work.doc_id);

		prepareDBToFetchData(); 

		function prepareDBToFetchData () {		
			mongodb.blindInitialize(function (dbx) {
				db = dbx; 
				dbx.collection(foundFilesCollName).findOne({"_id" : bsonId}, function (err, doc) {
					if (err || (doc == null)) {
						console.log('error retrieving document assigned to this worker, this document needs to be restarted');
						cluster.worker.disconnect(); 
					} else {
						if (updateFoundFileAssignedWorkerId) {
							
							var modifying = {
								$set : {
									a_wk : cluster.worker.id
								}
							}; 
							dbx.collection(foundFilesCollName).update({_id : bsonId}, modifying, {multi : false}, function (err, result) {
								if (err) {
									console.log('following error has ocurred updating workerId on foudFiles collection : ');
									console.log(err); 
									cluster.worker.disconnect(); 
								} else {
									if (callback) callback(doc); 
								}
							});
						} else {
							if (callback) callback(doc); 
						}
					}
				});
			});
		}
	}
}

function isEmpty(obj) {
    if (obj == null) return true;

	if (obj.length && obj.length > 0) return false;
	if (obj.length === 0)  return true;

    for (var key in obj) {
        if (hasOwnProperty.call(obj, key)) return false;
    }
 
    return true;
}






