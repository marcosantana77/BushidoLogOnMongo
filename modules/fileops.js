var lineReader = require('readline'); 
var fs = require('fs');
var _  = require('cloneextend');

var config = require('../config.js'); 
var parsetools = require('./parseops.js'); 

exports.getFilesListOnServer = function(server_id, url, callback) {

	var logFiles = []; 

	var fileObj = {
		s_id   : server_id,
		fn     : '',
		st     : 'available',
		a_wk   : ''
	}; 

	fs.readdir(url, function (err, files) {
	  if (err) {
	    console.log(err); 
	  }
	
	  var countDown = files.length; 
	  console.log('server #' + server_id + ' has ' + files.length + ' files.'); 

	  files.forEach( function (file) {
	  	var fileUrl = url + '/' + file; 

	  	if (file !== '.DS_Store') {
	  		var curFile = _.clone(fileObj);
	  		curFile.fn = fileUrl; 

	        logFiles.push(curFile); 
	  	}

	  	if (0===--countDown && callback) {
	  		callback(logFiles); 
		}
	  });
	}); 
}

exports.readLogFileWStream = function(docBsonId, filePath, callback) {

	var filename = filePath;
	var f_id = docBsonId; 

	var logEntries = [];

	lineReader.createInterface({
	    input: fs.createReadStream(filename),
	    terminal: false
	}).on('line', function(line){
		parsetools.getLogEntryObjFromStrLogLine(f_id, line, function(logEntryObj) {
		   logEntries.push(logEntryObj); 
		});
	}).on('close', function() {
		callback(null, logEntries); 
	});
}

exports.getWriteStream = function (user_id, callback) {

	var loadFilePath = config.runningArgs.loadDirName + '/' + user_id + '.log'; 

	var file = fs.createWriteStream(loadFilePath); 

	file.on('error', function (err) {
		console.log('following error ocurred on writting stream to file :' + loadFilePath); 
		console.log(err); 
		if (callback) callback(false); 
	});

	file.on('finish', function(){
  		console.log('file succefully saved to ', loadFilePath);
		if (callback) callback(true); 
	});

	return file; 
}