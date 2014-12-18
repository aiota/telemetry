var aiota = require("aiota-utils");
var amqp = require("amqp");
var jsonValidate = require("jsonschema").validate;
var MongoClient = require("mongodb").MongoClient;

var config = null;
var processName = "telemetry.js";
var dbConnection = null;
var storageQueue = [];

function validate(instance, schema)
{
	var v = jsonValidate(instance, schema);

	return (v.errors.length == 0 ? { isValid: true } : { isValid: false, error: v.errors });
}

function sampleTelemetry(elm)
{
	var collName = (elm.storage.hasOwnProperty("collectionName") ? elm.storage.collectionName : "telemetry_sample");

	dbConnection.collection(collName, function(error, collection) {
		if (error) {
			console.error(error);
			return;
		}

		var obj = { deviceId: elm.deviceId, timestamp: elm.timestamp };
		
		for (var prop in elm.payload) {
			obj[prop] = elm.payload[prop];
		}

		collection.insert(obj, function(err, result) {
			if (err) {
				console.log(err);
			}
		});
	});
}

function aggregateTelemetry(elm)
{
	var timestamp = 0;

	var dimension = [ 0, 0 ];
	var index = [ 0, 0 ];
	
	var date = new Date(elm.timestamp);
	var hour = date.getUTCHours();
	var minute = date.getUTCMinutes();
	var second = date.getUTCSeconds();

	var stats = (elm.storage.aggregation.hasOwnProperty("stats") ? elm.storage.aggregation.stats : false);
	var bucket = (elm.storage.hasOwnProperty("bucket") ? true : false);
	var bucketStats = (bucket ? (elm.storage.bucket.hasOwnProperty("stats") ? elm.storage.bucket.stats : false) : false);

	switch (elm.storage.aggregation.period) {
	case "minute":	timestamp = elm.timestamp - (elm.timestamp % 60000);
					break;
	case "hour":	timestamp = elm.timestamp - (elm.timestamp % 3600000);
					break;
	case "day":		timestamp = elm.timestamp - (elm.timestamp % 86400000);
					break;
	}
	
	if (bucket) {
		switch (elm.storage.bucket.period) {
		case "minute":	timestamp = elm.timestamp - (elm.timestamp % 60000);
						switch (elm.storage.bucket.entry) {
						case "second":		dimension = [ 60, 0 ];
											index = [ second, 0 ];
											break;
						}
						break;
		case "hour":	timestamp = elm.timestamp - (elm.timestamp % 3600000);
						switch (elm.storage.bucket.entry) {
						case "second":		dimension = [ 60, 60 ];
											index = [ minute, second ];
											break;
						case "minute":		dimension = [ 60, 0 ];
											index = [ minute, 0 ]
											break;
						}
						break;
		case "day":		timestamp = elm.timestamp - (elm.timestamp % 86400000);
						switch (elm.storage.bucket.entry) {
						case "minute":		dimension = [ 24, 60 ];
											index = [ hour, minute ];
											break;
						case "hour":		dimension = [ 24, 0 ];
											index = [ hour, 0 ];
											break;
						}
						break;
		}
	}

	var collName = (elm.storage.hasOwnProperty("collectionName") ? elm.storage.collectionName : "telemetry_aggregate_" + elm.storage.aggregation.period);
	
	dbConnection.collection(collName, function(error, collection) {
		if (error) {
			console.error(error);
			callback();
			return;
		}

		collection.findOne({ deviceId: elm.deviceId, timestamp: timestamp }, { _id: 1 }, function(err, result) {
			if (result) {
				var upd = { "$inc": {} };
			
				if (stats) {
					upd["$min"] = {};
					upd["$max"] = {};
				}

				if (bucket) {
					if (!stats && bucketStats) {
						upd["$min"] = {};
						upd["$max"] = {};
					}
					
					for (var prop in elm.payload) {
						if (dimension[1] == 0) {
							upd["$inc"]["values." + index[0] + "." + prop + ".count"] = 1;
						}
						else {
							upd["$inc"]["values." + index[0] + "." + index[1] + "." + prop + ".count"] = 1;
						}

						if (bucketStats) {
							upd["$inc"]["stats." + prop + ".count"] = 1;
						}

						switch (typeof(elm.payload[prop])) {
						case "number":	if (dimension[1] == 0) {
											upd["$inc"]["values." + index[0] + "." + prop + ".sum"] = elm.payload[prop];
											if (stats) {
												upd["$min"]["values." + index[0] + "." + prop + ".min"] = elm.payload[prop];
												upd["$max"]["values." + index[0] + "." + prop + ".max"] = elm.payload[prop];
											}
										}
										else {
											upd["$inc"]["values." + index[0] + "." + index[1] + "." + prop + ".sum"] = elm.payload[prop];
											if (stats) {
												upd["$min"]["values." + index[0] + "." + index[1] + "." + prop + ".min"] = elm.payload[prop];
												upd["$max"]["values." + index[0] + "." + index[1] + "." + prop + ".max"] = elm.payload[prop];
											}
										}
				
										if (bucketStats) {
											upd["$inc"]["stats." + prop + ".sum"] = elm.payload[prop];
											upd["$min"]["stats." + prop + ".min"] = elm.payload[prop];
											upd["$max"]["stats." + prop + ".max"] = elm.payload[prop];
										}
										break;

						case "object":	if (elm.payload[prop] instanceof Array) {
											for (var j = 0; j < elm.payload[prop].length; ++j) {
												if (dimension[1] == 0) {
													upd["$inc"]["values." + index[0] + "." + prop + ".sum." + j] = elm.payload[prop][j];
													if (stats) {
														upd["$min"]["values." + index[0] + "." + prop + ".min." + j] = elm.payload[prop][j];
														upd["$max"]["values." + index[0] + "." + prop + ".max." + j] = elm.payload[prop][j];
													}
												}
												else {
													upd["$inc"]["values." + index[0] + "." + index[1] + "." + prop + ".sum." + j] = elm.payload[prop][j];
													if (stats) {
														upd["$min"]["values." + index[0] + "." + index[1] + "." + prop + ".min." + j] = elm.payload[prop][j];
														upd["$max"]["values." + index[0] + "." + index[1] + "." + prop + ".max." + j] = elm.payload[prop][j];
													}
												}
						
												if (bucketStats) {
													upd["$inc"]["stats." + prop + ".sum." + j] = elm.payload[prop][j];
													upd["$min"]["stats." + prop + ".min." + j] = elm.payload[prop][j];
													upd["$max"]["stats." + prop + ".max." + j] = elm.payload[prop][j];
												}
											}						
										}
										else if (elm.payload[prop] instanceof Date) {
										}
										else {
											// This is a subdocument
										}
										break;
						}
					}
				}
				else {	
					for (var prop in elm.payload) {
						switch (typeof(elm.payload[prop])) {
						case "number":	upd["$inc"][prop + ".count"] = 1;
										upd["$inc"][prop + ".sum"] = elm.payload[prop];
										
										if (stats) {
											upd["$min"][prop + ".min"] = elm.payload[prop];
											upd["$max"][prop + ".max"] = elm.payload[prop];
										}
										break;

						case "object":	if (elm.payload[prop] instanceof Array) {
											upd["$inc"][prop + ".count"] = 1;
											for (var j = 0; j < elm.payload[prop].length; ++j) {
												upd["$inc"][prop + ".sum." + j] = elm.payload[prop][j];
												if (stats) {
													upd["$min"][prop + ".min." + j] = elm.payload[prop][j];
													upd["$max"][prop + ".max." + j] = elm.payload[prop][j];
												}										
											}
										}
										else if (elm.payload[prop] instanceof Date) {
										}
										else {
											// This is a subdocument
										}
										break;
						}
					}
				}
	
				collection.update({ _id: result._id }, upd, function(error, objects) {
					if (error) {
						console.error(error);
					}
				});
			}
			else {
				var obj = { deviceId: elm.deviceId, timestamp: timestamp };
				
				if (bucket) {
					obj.values = {};
					
					var val = {};
					var nval = {};
					
					for (var prop in elm.payload) {
						var nobj = 0;
						
						switch (typeof(elm.payload[prop])) {
						case "object":	if (elm.payload[prop] instanceof Array) {
											nobj = [];
											for (var i = 0; i < elm.payload[prop].length; ++i) {
												nobj.push(0);
											}
										}
										else if (elm.payload[prop] instanceof Date) {
										}
										else {
											// This is a subdocument
										}
										break;
						}
						
						val[prop] = { count: 1, sum: elm.payload[prop] };
						nval[prop] = { count: 0, sum: nobj };

						if (stats) {
							val[prop].min = elm.payload[prop];
							val[prop].max = elm.payload[prop];
							nval[prop].min = nobj;
							nval[prop].max = nobj;
						}						
					}
	
					if (dimension[1] == 0) {
						for (var i = 0; i < dimension[0]; ++i) {
							obj.values[i] = (i == index[0] ? val : nval);
						}
					}
					else {
						for (var i = 0; i < dimension[0]; ++i) {
							obj.values[i] = {};
						
							for (var j = 0; j < dimension[1]; ++j) {
								obj.values[i][j] = ((i == index[0]) && (j == index[1]) ? val : nval);
							}
						}
					}
					
					if (bucketStats) {
						obj.stats = val;
					}
				}
				else {
					for (var prop in elm.payload) {
						obj[prop] = { count: 1, sum: elm.payload[prop] };
	
						if (stats) {
							obj[prop].min = elm.payload[prop];
							obj[prop].max = elm.payload[prop];
						 }
					}
				}
				
				collection.insert(obj, function(err, result) {
					if (err) {
						console.log(err);
					}
				});
			}
		});
	});
}

function bucketTelemetry(elm)
{
	var timestamp = 0;

	var dimension = [ 0, 0 ];
	var index = [ 0, 0 ];
	
	var date = new Date(elm.timestamp);
	var hour = date.getUTCHours();
	var minute = date.getUTCMinutes();
	var second = date.getUTCSeconds();

	var stats = (elm.storage.bucket.hasOwnProperty("stats") ? elm.storage.bucket.stats : false);

	switch (elm.storage.bucket.period) {
	case "minute":	timestamp = elm.timestamp - (elm.timestamp % 60000);
					switch (elm.storage.bucket.entry) {
					case "second":		dimension = [ 60, 0 ];
										index = [ second, 0 ];
										break;
					}
					break;
	case "hour":	timestamp = elm.timestamp - (elm.timestamp % 3600000);
					switch (elm.storage.bucket.entry) {
					case "second":		dimension = [ 60, 60 ];
										index = [ minute, second ];
										break;
					case "minute":		dimension = [ 60, 0 ];
										index = [ minute, 0 ]
										break;
					}
					break;
	case "day":		timestamp = elm.timestamp - (elm.timestamp % 86400000);
					switch (elm.storage.bucket.entry) {
					case "minute":		dimension = [ 24, 60 ];
										index = [ hour, minute ];
										break;
					case "hour":		dimension = [ 24, 0 ];
										index = [ hour, 0 ];
										break;
					}
					break;
	}
			
	var collName = (elm.storage.hasOwnProperty("collectionName") ? elm.storage.collectionName : "telemetry_bucket_" + elm.storage.bucket.period);

	dbConnection.collection(collName, function(error, collection) {
		if (error) {
			console.error(error);
			callback();
			return;
		}

		collection.findOne({ deviceId: elm.deviceId, timestamp: timestamp }, { _id: 1 }, function(err, result) {
			if (result) {
				var set = {};
				
				if (dimension[1] == 0) {
					for (var prop in elm.payload) {
						set["values." + index[0] + "." + prop] = elm.payload[prop];
					}
				}
				else {
					for (var prop in elm.payload) {
						set["values." + index[0] + "." + index[1] + "." + prop] = elm.payload[prop];
					}
				}
		
				var upd = { $set: set };
				
				if (stats) {
					upd["$inc"] = {};
					upd["$min"] = {};
					upd["$max"] = {};
					
					for (var prop in elm.payload) {
						upd["$inc"]["stats." + prop + ".count"] = 1;

						switch (typeof(elm.payload[prop])) {
						case "number":	upd["$inc"]["stats." + prop + ".count"] = 1;
										upd["$inc"]["stats." + prop + ".sum"] = elm.payload[prop];
										upd["$min"]["stats." + prop + ".min"] = elm.payload[prop];
										upd["$max"]["stats." + prop + ".max"] = elm.payload[prop];
										break;

						case "object":	if (elm.payload[prop] instanceof Array) {
											upd["$inc"]["stats." + prop + ".count"] = 1;
											for (var j = 0; j < elm.payload[prop].length; ++j) {
												upd["$inc"]["stats." + prop + ".sum." + j] = elm.payload[prop][j];
												upd["$min"]["stats." + prop + ".min." + j] = elm.payload[prop][j];
												upd["$max"]["stats." + prop + ".max." + j] = elm.payload[prop][j];
											}						
										}
										else if (elm.payload[prop] instanceof Date) {
										}
										else {
											// This is a subdocument
										}
										break;
						}
					}
				}
				
				collection.update({ _id: result._id }, upd, function(error, objects) {
					if (error) {
						console.error(error);
					}
				});
			}
			else {
				var obj = {};
				var val = {};

				for (var prop in elm.payload) {
					switch (typeof(elm.payload[prop])) {
					case "number":	val[prop] = null;
									break;
					case "object":	if (elm.payload[prop] instanceof Array) {
										val[prop] = [];
										for (var i = 0; i < elm.payload[prop].length; ++i) {
											val[prop].push(null);
										}
									}
									else if (elm.payload[prop] instanceof Date) {
									}
									else {
										// This is a subdocument
									}
									break;
					}
				}
	
				if (dimension[1] == 0) {
					for (var i = 0; i < dimension[0]; ++i) {
						obj[i] = (i == index[0] ? elm.payload : val);
					}
				}
				else {
					for (var i = 0; i < dimension[0]; ++i) {
						obj[i] = {};
					
						for (var j = 0; j < dimension[1]; ++j) {
							obj[i][j] = ((i == index[0]) && (j == index[1]) ? elm.payload : val);
						}
					}
				}

				var ins = { deviceId: elm.deviceId, timestamp: timestamp, values: obj };
				
				if (stats) {
					ins["stats"] = {};
					
					for (var prop in elm.payload) {
						ins.stats[prop] = { count: 1, sum: elm.payload[prop], min: elm.payload[prop], max: elm.payload[prop] };
					}
				}
				
				collection.insert(ins, function(err, result) {
					if (err) {
						console.log(err);
					}
				});
			}
		});
	});
}

function storeTelemetry()
{
	if (storageQueue.length > 0) {
		var elm = storageQueue.shift();

		if (elm.storage.hasOwnProperty("sample")) {
			sampleTelemetry(elm);
		}
		else if (elm.storage.hasOwnProperty("aggregation")) {
			aggregateTelemetry(elm);
		}
		else if (elm.storage.hasOwnProperty("bucket")) {
			bucketTelemetry(elm);
		}
		
		process.nextTick(storeTelemetry);
	}
}

function handleTelemetryRequest(db, msg, callback)
{
	for (var i = 0; i < msg.storage.length; ++i) {
		storageQueue.push({ deviceId: msg.header.deviceId, timestamp: msg.header.timestamp, storage: msg.storage[i], payload: msg.body });
	}
	
	process.nextTick(storeTelemetry);
	
	if (msg.updateDevice) {
		// Update device with latest values
		var set = {};

		for (var prop in msg.body) {
			set["telemetry." + prop] = { value: msg.body[prop], lastUpdate: msg.header.timestamp };
		}

		db.collection("devices", function(err, collection) {
			if (err) {
				callback({ error: err, errorCode: 200001 });
				return;
			}
			
			collection.update({ _id: msg.header.deviceId }, { $set: set }, function(err, result) {
				callback((err ? { error: err, errorCode: 200004 } : { status: "OK" }));
			});
		});
	}
	else {
		callback({ status: "OK" });
	}
}

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, aiotaDB) {
	if (err) {
		aiota.log(processName, "", null, err);
	}
	else {
		aiota.getConfig(aiotaDB, function(c) {
			if (c == null) {
				aiota.log(processName, "", aiotaDB, "Error getting config from database");
			}
			else {
				config = c;

				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, db) {
					if (err) {
						aiota.log(processName, config.serverName, aiotaDB, err);
					}
					else {
						dbConnection = db;
						var bus = amqp.createConnection(config.amqp);
						
						bus.on("ready", function() {
							var cl = { group: "device", type: "telemetry" };
							bus.queue(aiota.getQueue(cl), { autoDelete: false, durable: true }, function(queue) {
								queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
									handleTelemetryRequest(db, msg, function(result) {
										queue.shift();
									});
								});
							});
						});
		
						setInterval(function() { aiota.heartbeat(processName, config.serverName, aiotaDB); }, 10000);
		
						process.on("SIGTERM", function() {
							aiota.terminateProcess(processName, config.serverName, aiotaDB, function() {
								process.exit(1);
							});
						});
					}
				});
			}
		});
	}
});
