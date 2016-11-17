// When want to change the queries, run this code using:
// node ~/workspace-mars/node_example/Write2HDFS_query.js
// One new file will be written out to /home/hduser1/spark/sparkExamples/output/queries/
// and /user/hduser1/sparkExamples/output/queries/.

var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient();
var hdfs = require('./webhdfs-client');

var fs = require('fs');


var outer = function(callback) {
	var that_machineId = 1;
	var that_location = 1;
	return function() {
		try {

			var timestamp = Number(new Date());

			console.log("create one file at : " + timestamp);

			var localFileName = '/home/hduser1/spark/sparkExamples/output/queries/'
					+ that_machineId + '_' + timestamp + '.csv';
			var hdfsFileName = '/user/hduser1/sparkExamples/output/queries/'
					+ that_machineId + timestamp + '.csv';
			// {"machineid":"m1", "location":"",
			// "temperature":89,"timestamp":"2016-01-01 11:11:11"}
			fs.appendFile(localFileName, "select * from dummy" + "\n",
					function(err) {
						if (err) {
							return console.log(err);
						}
					});
			var localFileStream = fs.createReadStream(localFileName);
			localFileStream.on('error', function(error) {
				console.log("--->Caught in createReadStream: " + error + "\n");
			});

			var remoteFileStream = hdfs.createWriteStream(hdfsFileName);
			// Pipe data to HDFS
			console.log(hdfsFileName);
			localFileStream.pipe(remoteFileStream);

			;
		} catch (err) {
			callback(err);
		}
	}
}

for (var i = 0; i <= 0; i++) {

	setTimeout(outer(printError), 1);

};

function printError(err) {
	console.log(err);
}