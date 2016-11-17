var WebHDFS = require('webhdfs');
var hdfs = WebHDFS.createClient();
var hdfs = require('./webhdfs-client');

var fs = require('fs');
var chance = require('chance')

// Load Chance
var Chance = require('chance');

// Instantiate Chance so it can be used
var chance = new Chance();

var outer = function(callback) {
	var that_machineId = machineId;
	var that_location = location;
	var that_current_state = current_state;
;
	return function() {
		try {
			var that_timestamp = Math.floor(new Date() / 1000);

			var localFileName = '/home/hduser1/spark/sparkExamples/output/databricks_1/'
					+ that_machineId + '_' + that_timestamp + '.json';
			var hdfsFileName = '/user/hduser1/sparkExamples/output/databricks_1/'
					+ that_machineId + that_timestamp + '.json';

			fs.appendFile(localFileName, 
					//{"time":1469501107,"action":"Open"}
					"{\"time\":"  + that_timestamp + ",\"action\":\"" + that_current_state + "\"} "
					+ "\n", function(err) {
				if (err) {
					return console.log(err);
				}
			});
			console.log("{\"time\":"  + that_timestamp + ",\"action\":\"" + that_current_state + "\"} ")
			var localFileStream = fs.createReadStream(localFileName);
			localFileStream.on('error', function(error) {
				console.log("--->Caught in createReadStream: " + error + "\n");
			});

			var remoteFileStream = hdfs.createWriteStream(hdfsFileName);
			// Pipe data to HDFS
//			console.log(hdfsFileName);
			localFileStream.pipe(remoteFileStream);
			
		} catch (err) {
			callback(err);
		}
	}
}

var machineIds = [ 1886, 4509, 9048, 3529, 2823 ];
var machineLocations = ["131 Asmun Pike Iklakja", "261 Pawnaf Highway Tupawawe", "659 Ujiini Park", "1104 Gila Turnpike", "798 Okbep Heights",];

var tenMillion = 1000000;
var prev_state = "close";
var rand_num_open = getRandomArbitrary(1, 10);
var num_open_sofar = 0;
var num_open_todo = 0;
var current_state;
var num_open_total = 0;
var num_close_total = 0;

//for (var i = tenMillion - 1; i >= 0; i--) {
	for (var i = 5000; i >= 0; i--) {
		console.log("what is i: " + i)
	if (num_open_sofar == 0){
		num_open_todo = Math.floor(getRandomArbitrary(2, 10));	
		console.log("at check 1: " + num_open_todo)
		current_state = 'close';
	}
	
	console.log("===> todo: " + num_open_todo + ", so far: " + num_open_sofar + ", state: " + current_state)
	if (current_state=="close"){
		num_close_total = num_close_total + 1;
	}else{
		num_open_total = num_open_total +1;
	}
	var randomIndex = getRandomIntInclusive(0, machineIds.length - 1);
//	var machineId = machineIds[getRandomIntInclusive(0, machineIds.length - 1)];
	var machineId = machineIds[randomIndex];
	var location = machineLocations[randomIndex];
	setTimeout(outer(printError), i* 1000 * getRandomArbitrary(1,30));
	// Very fast
	if (num_open_sofar == num_open_todo - 1){
		num_open_sofar = 0;
//		console.log("at check 2: " + num_open_todo)
//		console.log("at check 3: " + num_open_sofar)
	}else{
//		console.log("at check 4: " + num_open_todo)
//		console.log("at check 5: " + num_open_sofar)
		num_open_sofar = num_open_sofar + 1;
//		console.log("at check 6: " + num_open_todo)
//		console.log("at check 7: " + num_open_sofar)
		current_state = 'open';		
	}
//	console.log("number of total opens: " , num_open_total);
//	console.log("number of total closes: " , num_close_total);
};

function wait(ms){
	   var start = new Date().getTime();
	   var end = start;
	   while(end < start + ms) {
	     end = new Date().getTime();
	  }
	}
function getRandomArbitrary(min, max) {
	return Math.random() * (max - min) + min;
}
function getRandomIntInclusive(min, max) {
	// return Math.floor(Math.random() * (max - min + 1)) + min;
	return Math.floor(Math.random() * (max - min + 1) + min);
}
function printError(err) {
//	console.log(err);
}