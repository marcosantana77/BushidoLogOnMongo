var cluster = require('cluster');
var mongo = require('mongodb'); 
var BSON = mongo.BSONPure; 
var _  = require('cloneextend');

var config = require('./config.js');
var performer = require('./performing.js'); 

var clustertools = require('./modules/clusterops.js'); 
var mongodb = require('./modules/mongoops.js'); 
var fstools = require('./modules/fileops.js'); 

var db; 

var userAndWorker = {
	status : 'available'  // available => on => done
};

var usersList = [];

var logEntriesCollName = config.mongo.logEntriesCollName,
	foundFilesCollName = config.mongo.foundFilesCollName; 

var numCPUs = config.environment.CPUs; 


exports.perform = function(dbx, callback) {

	db = dbx; 

	assessUsersAndSetUpUsersList(function (jobDone) {

		if (jobDone) {
			setUpInitialWorkers(function (jobDone) {
				callback(jobDone); 
			});
		} else {
			callback(false); 
		}
	});

	function assessUsersAndSetUpUsersList (callback) {

		getUserList(function (err, userIds) {
			var countDown = userIds.length; 
			if (err) {
				console.log('following error has ocurred returning userIds list: ' + err.code);
				callback(false); 
			} else {
				userIds.forEach(function (item, index) {
					var userAndWorkerObj = _.clone(userAndWorker);
					userAndWorkerObj.user_id = item; 

					usersList.push(userAndWorkerObj); 

					if (0===--countDown && callback) callback(true); 
				});
			}
		}); 

		function getUserList (callback) {
			db.collection(logEntriesCollName).distinct('u_id', {}, function(err, array) {
				console.log('user list after distinct');
				console.log(array); 
				callback(err, array); 
			});
		}
	}

	function setUpInitialWorkers (callback) {

		performer.runThroughUsersListAndGetNexAvailableUser(numCPUs, function (usersListToWorkOn) {

			if (usersListToWorkOn == null) {
				//no further work to be done
				callback(false); 
			} else {
				var countDown = numCPUs; 
				usersListToWorkOn.forEach(function (item, index) {
					clustertools.initializeWorker(false, item.user_id, index);
					if (0===--countDown && callback) callback (true); 
				});
			}
		}); 
	}
}

exports.getDataFromLogEntriesCollectionAndSaveFile = function (user_id, callback) {

	performer.getFileContentForThisListUserIdAndWriteStream(user_id, function (err) {

		if (err) {
			console.log('following error when returning the log list for user ' + user_id + ':');
			console.log(err); 
			if (callback) callback(false); 
		} else {
			if (callback) callback(true); 

			//TODO remove info for this user... 
		}
	});
}

exports.runThroughUsersListAndGetNexAvailableUser = function (usersNumber, callback) {

	var usersToCurrentWork = [];

	for (i in usersList) {
		var user = usersList[i];

		if (user.status === 'available') {
			user.status = 'on'; 
			usersToCurrentWork.push(user); 
		}
		if (usersToCurrentWork.length == usersNumber) break; 
	}

	if (!usersToCurrentWork.length) {
		callback(null);
	} else {
		callback(usersToCurrentWork); 
	}
}

exports.getFileContentForThisListUserIdAndWriteStream = function (user_id, callback) {

	console.log('returning info and saving log file for this user:');
	console.log (user_id + '\n') ; 

	mongodb.blindInitialize(function (dbx) {

		var cursor = dbx.collection(logEntriesCollName).find({ u_id : user_id }, { _id : false, ll : true }).sort({dt : 1}); 

		var file = fstools.getWriteStream(user_id, function(jobDone) {
			callback(null); 
		});

		cursor.each(function (err, item) {
			if (err) {
				callback(err);
			} else {
				if (item === null) {
					file.end(); 
				} else {
					file.write(item.ll + '\n'); 
				}
			}
		});
	});
}
