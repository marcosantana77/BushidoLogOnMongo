
exports.getLogEntryObjFromStrLogLine = function (f_id, logLine, callback) {

	var logEntryObj = {
		f_id : f_id, 
		ll : logLine 
	}

	logEntryObj.u_id = getUserId(logLine); 
	logEntryObj.dt = getDataEntryDate(logLine);

	callback(logEntryObj); 
}

function getUserId (logLine) {

	var indexOfUserId = logLine.indexOf('\"userid=') + 8; 

	var userId = logLine.substring(indexOfUserId, logLine.length - 1)

	return userId; 
}

function getDataEntryDate (logLine) {

	var indexOfBeginningOfDate = logLine.indexOf(' - - [') + 6; 
	var indexOfEndOfDate = logLine.indexOf('] \"GET \/'); 

	var rawDataEntryDate = logLine.substring(indexOfBeginningOfDate, indexOfEndOfDate); 

	//replaces bad colon between date and time with a space
	var refinedDataEntryDate = rawDataEntryDate.substring(0, 11) + ' ' + rawDataEntryDate.substring(12, rawDataEntryDate.length); 

	return new Date(refinedDataEntryDate); 
}