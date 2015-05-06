var cluster = require('cluster');

exports.initializeWorker = function (isAssessment, prop_id, workerId, callback) {

	var prop = (isAssessment ? 'doc_id' : 'user_id'); 
	console.log('about to fork performer worker #' + workerId); 

	var newWorker = cluster.fork(workerId);
	
	newWorker.assignedWorkerId = workerId; 
	newWorker[prop] = prop_id;
	newWorker.isAssessment = isAssessment; 
}
