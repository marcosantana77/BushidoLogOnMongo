module.exports = {
	mongo : {
		connectionString 	: 'mongodb://localhost:27017/logHunter',
		foundFilesCollName  : 'foundFiles',
		logEntriesCollName  : 'logEntries'
	},
	runningArgs : {
		logDirNamePrefix	: 'srv', 	//app won't assume the unexistance of the logDirNamePrefix directories. fatal exception shall be thrown
		farmSize			: 4,  		//replace this for as many servers with log files there is a which to fetch data from. see further notes at the end of this file
		loadDirName			: __dirname + '/loaded' 	//destination for reduced files
	},
	environment	: {
		CPUs 				: require('os').cpus().length //replace this for creating as many concurrent workers as desired, standard is the amount of processors/cores
	}
}

//runningArgs.farmSize could be also a collection of servers with actual ip adresses and directory path, app is not ready to accomplish such change
//but it can be easily adapted to do so

//suppose structure for actual servers below (app not implemented to support such feature)
// runningArgs : {
// 		logDirNamePrefix	: 'srv', //deprecated with actual servers
// 		farmLogServers      : [
// 			{
// 				ip				: '192.168.0.7',
// 				dirName 		: 'logPathOne'
// 			},
// 			{
// 				ip 				: '192.168.0.17',
// 				dirName 		: 'anotherPathForAnotherServer'
// 			}
// 		], // and so on and on as many as needed  
// 		farmSize			: this.farmLogServers.length,  
// 		loadDirName			: 'loaded'
// 	}