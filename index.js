/***
 * Copyright 2016 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file Re-usable functions for interacting with IA's REST API
 * @license Apache-2.0
 * @requires xmldom
 * @requires xpath
 * @requires ibm-igc-rest
 * @example
 * // runs column analysis for any objects in Automated Profiling that have not been analyzed since the moment the script is run (new Date())
 * var iarest = require('ibm-ia-rest');
 * iarest.setAuth("isadmin", "isadmin");
 * iarest.setServer("hostname", "9445");
 * iarest.getStaleAnalysisResults("Automated Profiling", new Date(), function(errStale, aStaleSources) {
 *   iarest.runColumnAnalysisForSources("Automated Profiling", aStaleSources, function(errCA, resCA) {
 *     var aExecIDs = iarest.getExecutionIDsFromResponse(resCA);
 *     // Note that the API returns async; if you want to busy-wait you need to poll the execution ID
 *   });
 * });
 */

/**
 * @module ibm-ia-rest
 */

const https = require('https');
const xmldom = require('xmldom');
const xpath = require('xpath');
var igcrest = require('ibm-igc-rest');

var auth = "";
var host = "";
var port = "";

const ignoreLabelName = "Information Analyzer Ignore List";
const ignoreLabelDesc = "Information Analyzer should ignore any assets with this label; they should not be indexed.";

/**
 * Set authentication details to access the REST API
 *
 * @param {string} user - the username
 * @param {string} password - the user's password
 * @returns {string} authentication in the form of 'user:password'
 */
exports.setAuth = function(user, password) {
  if (user === undefined || user === "" || password === undefined || password === "") {
    throw new Error("Incomplete authentication information -- missing username or password (or both).");
  }
  this.auth = user + ":" + password;
  igcrest.setAuth(user, password);
  return this.auth;
}

/**
 * Set access details for the REST API
 *
 * @param {string} host - the hostname of the domain (services) tier of the IGC server
 * @param {string} port - the port number of the IGC server (e.g. 9445)
 */
exports.setServer = function(host, port) {
  this.host = host;
  this.port = port;
  igcrest.setServer(host, port);
}

/**
 * @private
 */
function _getValueOrDefault(val, def) {
  return (val === undefined) ? def : val
}

/**
 * @namespace
 */

/**
 * @constructor
 */
function Project(name) {
  this.doc = new xmldom.DOMImplementation().createDocument("http://www.ibm.com/investigate/api/iaapi", "iaapi:Project", null);
  this.doc.documentElement.setAttribute("xmlns:iaapi", "http://www.ibm.com/investigate/api/iaapi");
  this.doc.documentElement.setAttribute("name", name);
  this.doc.normalize();
}
Project.prototype = {

  doc: null,

  /**
   * Retrieve the Project document
   * 
   * @function
   */
  getProjectDoc: function() {
    return this.doc;
  },

  /**
   * Set the description of the project
   *
   * @function
   */
  setDescription: function(desc) {
    var eDesc = this.doc.createElement("description");
    var txt = this.doc.createTextNode(desc);
    eDesc.appendChild(txt);
    this.doc.documentElement.appendChild(eDesc);
  },

  /**
   * Add the specified table to the project
   *
   * @function
   * @param {string} datasource - the database name 
   * @param {string} schema
   * @param {string} table
   * @param {string[]} aColumns - array of column names
   */
  addTable: function(datasource, schema, table, aColumns) {
    
    var nDS = this.doc.getElementsByTagName("DataSources");
    if (nDS.length == 0) {
      nDS = this.doc.createElement("DataSources");
      this.doc.documentElement.appendChild(nDS);
    } else {
      nDS = nDS[0];
    }
    
    var eDS = this.doc.createElement("DataSource");
    eDS.setAttribute("name", datasource);
    nDS.appendChild(eDS);
    var eS = this.doc.createElement("Schema");
    eS.setAttribute("name", schema);
    eDS.appendChild(eS);
    var eT = this.doc.createElement("Table");
    eT.setAttribute("name", table);
    eS.appendChild(eT);

    for (var i = 0; i < aColumns.length; i++) {
      var sColName = aColumns[i];
      var eC = this.doc.createElement("Column");
      eC.setAttribute("name", sColName);
      eT.appendChild(eC);
    }

  }

};

/**
 * @namespace
 */

/**
 * @constructor
 * @param {Project} project - the project in which to create the column analysis task
 * @param {boolean} analyzeColumnProperties - whether or not to analyze column properties
 * @param {string} captureResultsType - specifies the type of frequency distribution results that are written to the analysis database ["CAPTURE_NONE", "CAPTURE_ALL", "CAPTURE_N"]
 * @param {int} minCaptureSize - the minimum number of results that are written to the analysis database, including both typical and atypical values
 * @param {int} maxCaptureSize - the maximum number of results that are written to the analysis database
 * @param {boolean} analyzeDataClasses - whether or not to analyze data classes
 */
function ColumnAnalysis(project, analyzeColumnProperties, captureResultsType, minCaptureSize, maxCaptureSize, analyzeDataClasses) {

  this.doc = project.getProjectDoc();
    
  analyzeColumnProperties = _getValueOrDefault(analyzeColumnProperties, true);
  captureResultsType = _getValueOrDefault(captureResultsType, "CAPTURE_ALL");
  minCaptureSize = _getValueOrDefault(minCaptureSize, 5000);
  maxCaptureSize = _getValueOrDefault(maxCaptureSize, 10000);
  analyzeDataClasses = _getValueOrDefault(analyzeDataClasses, false);

  var eRCA = this.doc.createElement("RunColumnAnalysis");
  eRCA.setAttribute("analyzeColumnProperties", analyzeColumnProperties);
  eRCA.setAttribute("captureFDResultsType", captureResultsType);
  eRCA.setAttribute("minFDCaptureSize", minCaptureSize);
  eRCA.setAttribute("maxFDCaptureSize", maxCaptureSize);
  eRCA.setAttribute("analyzeDataClasses", analyzeDataClasses);

  var task = this.doc.getElementsByTagName("Tasks");
  if (task.length == 0) {
    task = this.doc.createElement("Tasks");
    this.doc.documentElement.appendChild(task);
  } else {
    task = task[0];
  }

  task.appendChild(eRCA);

};
ColumnAnalysis.prototype = {

  doc: null,
  sampleOpts: null,

  /**
   * Use to (optionally) set any sampling options for the column analysis
   *
   * @function
   * @param {string} type - the sampling type ["random", "sequential", "every_nth"]
   * @param {number} size - if less than 1.0, the percentage of values to use in the sample; otherwise the maximum number of records in the sample.  If you use the "random" type of data sample, specify the sample size that is the same number as the number of records that will be in the result, based on the value that you specify in the Percent field. Otherwise, the results might be skewed.
   * @param {string} seed - if type is "random", this value is used to initialize the random generators (two samplings that use the same seed value will contain the same records)
   * @param {int} step - if type is "every_nth", this value indicates the step to apply (one row will be kept out of every nth value rows)
   */
  setSampleOptions: function(type, size, seed, step) {
    var eSO = this.doc.createElement("SampleOptions");
    eSO.setAttribute("type", type);
    if (size <= 1.0) {
      eSO.setAttribute("percent", size);
    } else {
      eSO.setAttribute("size", size);
    }
    if (type === "random") {
      eSO.setAttribute("seed", seed);
    } else if (type == "every_nth") {
      eSO.setAttribute("nthValue", step);
    }
    this.doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eSO);
  },

  /**
   * Use to (optionally) set any engine options to use when running the column analysis
   *
   * @function
   * @param {boolean} retainOSH - whether to retain the generated DataStage job or not
   * @param {boolean} retainData - whether to retain generated data sets (ignored when data rules are running)
   * @param {string} config - specifies an alternative configuration file to use with the DataStage engine during this run
   * @param {string} gridEnabled - whether or not the grid view will be enabled
   * @param {string} requestedNodes - the name of requested nodes
   * @param {string} minNodes - the minimum number of nodes you want in the analysis
   * @param {string} partitionsPerNode - the number of partitions for each node in the analysis
   */
  setEngineOptions: function(retainOSH, retainData, config, gridEnabled, requestedNodes, minNodes, partitionsPerNode) {
    var eEO = this.doc.createElement("EngineOptions");
    eEO.setAttribute("retainOsh", retainOSH);
    eEO.setAttribute("retainDataSets", retainData);
    eEO.setAttribute("PXConfigurationFile", config);
    eEO.setAttribute("gridEnabled", gridEnabled);
    eEO.setAttribute("requestedNodes", requestedNodes);
    eEO.setAttribute("minimumNodes", minNodes);
    eEO.setAttribute("partitionsPerNode", partitionsPerNode);
    this.doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eEO);
  },

  /**
   * Use to (optionally) set any job options to use when running the column analysis
   *
   * @function
   * @param {boolean} debugEnabled - whether to generate a debug table containing the evaluation results of all functions and tests contained in the expression (only used for running data rules)
   * @param {int} numDebuggedRecords - how many rows should be debugged, if debugEnabled is "true"
   * @param {int} arraySize - the size of the array (?)
   * @param {boolean} autoCommit
   * @param {int} isolationLevel
   * @param {boolean} updateExistingTables - whether to update existing tables in IADB or create new ones (only used for column analysis)
   */
  setJobOptions: function(debugEnabled, numDebuggedRecords, arraySize, autoCommit, isolationLevel, updateExistingTables) {
    var eJO = this.doc.createElement("JobOptions");
    eJO.setAttribute("debugEnabled", debugEnabled);
    eJO.setAttribute("nbOfDebuggedRecords", numDebuggedRecords);
    eJO.setAttribute("arraySize", arraySize);
    eJO.setAttribute("autoCommit", autoCommit);
    eJO.setAttribute("isolationLevel", isolationLevel);
    eJO.setAttribute("updateExistingTables", updateExistingTables);
    this.doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eJO);
  },

  /**
   * Use to add a column to the column analysis task -- both table and column can be '*' to specify all tables or all columns
   *
   * @function
   * @param {string} datasource
   * @param {string} schema
   * @param {string} table
   * @param {string} column
   * @param {string} [hostname]
   */
  addColumn: function(datasource, schema, table, column, hostname) {
    var name = datasource + "." + schema + "." + table + "." + column;
    // TODO: determine correct way of specifying fully-qualified name that includes hostname
    /* NOTE: hostname cannot be pre-pended with a "." separator -- hostname itself has .s in it -- results in 500 response code
    if (hostname !== undefined) {
      name = hostname + "." + name;
    }
    */
    var eC = this.doc.createElement("Column");
    eC.setAttribute("name", name);
    this.doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eC);
  },

  /**
   * Use to add a file field to the column analysis task -- column can be '*' to specify all fields within the file
   *
   * @function
   * @param {string} connection - e.g. "HDFS"
   * @param {string} path - directory path, not including the filename
   * @param {string} filename
   * @param {string} column - name of the field within the file
   * @param {string} [hostname]
   */
  addFileField: function(connection, path, filename, column, hostname) {
    var name = connection + ":" + path + ":" + filename + ":" + column;
    if (hostname !== undefined) {
      name = hostname + ":" + name;
    }
    var eF = this.doc.createElement("Column");
    eF.setAttribute("name", name);
    this.doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eF);
  }

};

/**
 * @namespace
 */

/**
 * @constructor
 * @param {Project} project - the project from which to publish analysis results
 */
function PublishResults(project) {

  this.doc = project.getProjectDoc();
    
  var ePR = this.doc.createElement("PublishResults");

  var task = this.doc.getElementsByTagName("Tasks");
  if (task.length == 0) {
    task = this.doc.createElement("Tasks");
    this.doc.documentElement.appendChild(task);
  } else {
    task = task[0];
  }

  task.appendChild(ePR);

};
PublishResults.prototype = {

  doc: null,

  /**
   * Use to add a table whose results should be published -- the table can be '*' to specify all tables
   *
   * @function
   * @param {string} datasource
   * @param {string} schema
   * @param {string} table
   * @param {string} [hostname]
   */
  addTable: function(datasource, schema, table, hostname) {
    var name = datasource + "." + schema + "." + table;
    // TODO: determine correct way of specifying fully-qualified name that includes hostname
    /* NOTE: hostname cannot be pre-pended with a "." separator -- hostname itself has .s in it -- results in 500 response code
    if (hostname !== undefined) {
      name = hostname.toUpperCase() + "." + name;
    }
    */
    var eC = this.doc.createElement("Table");
    eC.setAttribute("name", name);
    this.doc.getElementsByTagName("PublishResults").item(0).appendChild(eC);
  },

  /**
   * Use to add a file whose results should be published -- file can be '*' to specify all files
   *
   * @function
   * @param {string} connection - e.g. "HDFS"
   * @param {string} path - directory path, not including the filename
   * @param {string} filename
   * @param {string} [hostname]
   */
  addFile: function(connection, path, filename, hostname) {
    var name = connection + ":" + path + ":" + filename;
    if (hostname !== undefined) {
      name = hostname.toUpperCase() + ":" + name;
    }
    var eF = this.doc.createElement("Table");
    eF.setAttribute("name", name);
    this.doc.getElementsByTagName("PublishResults").item(0).appendChild(eF);
  }

};

if (typeof require == 'function') {
  exports.Project = Project;
  exports.ColumnAnalysis = ColumnAnalysis;
  exports.PublishResults = PublishResults;
}

/**
 * Make a request against IA's REST API
 *
 * @see module:ibm-ia-rest.setServer
 * @see module:ibm-ia-rest.setAuth
 * @param {string} method - type of request, one of ['GET', 'PUT', 'POST', 'DELETE']
 * @param {string} path - the path to the end-point (e.g. /ibm/iis/ia/api/...)
 * @param {string} [input] - any input for the request, i.e. for PUT, POST
 * @param {requestCallback} callback - callback that handles the response
 * @throws will throw an error if connectivity details are incomplete or there is a fatal error during the request
 */
exports.makeRequest = function(method, path, input, callback) {

  var bInput = (typeof input !== 'undefined' && input !== null);
  
  if (this.auth == "" || this.host == "" || this.port == "") {
    throw new Error("Setup incomplete: auth = " + this.auth + ", host = " + this.host + ", port = " + this.port + ".");
  }

  var opts = {
    auth: this.auth,
    hostname: this.host,
    port: this.port,
    path: path,
    method: method,
    rejectUnauthorized: false,
    maxSockets: 1,
    keepAlive: false
  }
  if (bInput) {
    opts.headers = {
      'Content-Type': 'text/xml',
      'Content-Length': input.length
    }
  }
  opts.agent = new https.Agent(opts);

  var req = https.request(opts, (res) => {

    var data = "";
    res.on('data', (d) => {
      data += d;
    });
    res.on('end', function() {
      callback(res, data);
      return data;
    });
  });
  if (bInput) {
    req.write(input);
  }
  req.end();

  req.on('error', (e) => {
    throw new Error(e);
  });

}

/**
 * @private
 */
function _getAllHostsWithDatabases(callback) {
  
  var json = {
    "pageSize": "10000",
    "properties": [ "name" ],
    "types": [ "host" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "databases",
          "operator": "isNull",
          "negated": true
        },
        {
          "property": "labels.name",
          "operator": "=",
          "value": ignoreLabelName,
          "negated": true
        }
      ]
    }
  }

  igcrest.search(json, function (err, resSearch) {

    var toReturn = [];
    for (var i = 0; i < resSearch.items.length; i++) {
      var item = resSearch.items[i];
      var sHostName = item._name;
      toReturn.push(sHostName);
    }
    callback(err, toReturn);

  });

}

/**
 * @private
 */
function _getAllDatabasesAndSchemasForHost(hostname, callback) {

  var json = {
    "pageSize": "10000",
    "properties": [ "name", "database_schemas.name", "host.name" ],
    "types": [ "database" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "host.name",
          "operator": "=",
          "value": hostname
        },
        {
          "property": "labels.name",
          "operator": "=",
          "value": ignoreLabelName,
          "negated": true
        }
      ]
    }
  };

  igcrest.search(json, function (err, resSearch) {

    callback(err, resSearch);

  });

}

/**
 * @private
 */
function _getAllSchemasForDatabase(hostname, datasource, callback) {

  var json = {
    "pageSize": "10000",
    "properties": [ "name", "database_schemas.name", "host.name" ],
    "types": [ "database" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "name",
          "operator": "=",
          "value": datasource
        },
        {
          "property": "host.name",
          "operator": "=",
          "value": hostname
        },
        {
          "property": "labels.name",
          "operator": "=",
          "value": ignoreLabelName,
          "negated": true
        }
      ]
    }
  };

  igcrest.search(json, function (err, resSearch) {

    callback(err, resSearch);

  });

}

/**
 * @private
 */
function _getAllTablesAndColumnsForSchema(hostname, datasource, schema, updatedAfter, callback) {

  var json = {
    "pageSize": "10000",
    "properties": [ "name", "database_columns.name", "database_schema.name", "database_schema.database.name", "database_schema.database.host.name" ],
    "types": [ "database_table" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "database_schema.name",
          "operator": "=",
          "value": schema
        },
        {
          "property": "database_schema.database.name",
          "operator": "=",
          "value": datasource
        },
        {
          "property": "database_schema.database.host.name",
          "operator": "=",
          "value": hostname
        },
        {
          "property": "labels.name",
          "operator": "=",
          "value": ignoreLabelName,
          "negated": true
        }
      ]
    }
  };

  if (updatedAfter !== undefined && updatedAfter !== "") {
    json.where.conditions.push({
      "property": "modified_on",
      "operator": ">=",
      "value": updatedAfter.valueOf()
    });
  }

  igcrest.search(json, function (err, resSearch) {
  
    callback(err, resSearch);

  });

}

/**
 * Retrieves a list of all items that should be ignored, i.e. where they are labelled with "Information Analyzer Ignore List"
 *
 * @param {itemsToIgnoreCallback} callback
 */
exports.getAllItemsToIgnore = function(callback) {

  // NOTE: the query below looks backwards with 'negated=false', but unfortunately only seems to work this way
  var json = {
    "pageSize": "10000",
    "properties": [ "name" ],
    "types": [ "host", "database", "database_schema", "database_table", "database_column", "data_file_folder", "data_file" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "labels.name",
          "operator": "=",
          "value": ignoreLabelName,
          "negated": false
        }
      ]
    }
  };

  igcrest.search(json, function (err, resSearch) {
    var typesToItems = {
      "host": [],
      "database": [],
      "database_schema": [],
      "database_table": [],
      "database_column": [],
      "data_file_folder": [],
      "data_file": []
    };
    for (var i = 0; i < resSearch.items.length; i++) {
      var item = resSearch.items[i];
      var type = item._type;
      typesToItems[type].push(igcrest.getItemIdentityString(item));
    }
    callback(err, typesToItems);
  });

}

/**
 * @private
 */
function _createOrUpdateProjectRequest(inputXML, bCreate, callback) {
  var endpoint = (bCreate) ? "/ibm/iis/ia/api/create" : "/ibm/iis/ia/api/update"
  exports.makeRequest('POST', endpoint, inputXML, function(res, resCreate) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, resCreate);
    return resCreate;
  });
}

/**
 * @private
 */
function _createOrUpdateIgnoreList(callback) {

  //var labelName = "Information Analyzer Ignore List";
  //var labelDesc = "Information Analyzer should ignore any assets with this label; they should not be indexed.";
  var queryLabelExistence = {
    "properties": [ "name" ],
    "types": [ "label" ],
    "where":
    {
      "operator": "and",
      "conditions":
      [
        {
          "property": "name",
          "operator": "=",
          "value": ignoreLabelName
        }
      ]
    }
  };

  igcrest.search(queryLabelExistence, function (err, resSearch) {
  
    var labelRID = "";
    for (var i = 0; i < resSearch.items.length; i++) {
      var item = resSearch.items[i];
      if (item.hasOwnProperty("_id")) {
        labelRID = item._id;
      }
    }

    if (labelRID === "") {
      igcrest.create('label', {'name': ignoreLabelName, 'description': ignoreLabelDesc}, function(res, labelRID) {
        callback(labelRID);
      });
    } else {
      callback(labelRID);
    }
  
  });

}

/**
 * Adds the IADB schema to a list of objects for Information Analyzer to ignore (to prevent them being added to projects or being analysed); this is accomplished by creating a label 'InformationAnalyzer'
 *
 * @param {requestCallback} callback - callback that handles the response
 */
exports.addIADBToIgnoreList = function(callback) {
  
  exports.makeRequest('GET', "/ibm/iis/ia/api/getIADBParams", null, function(res, resJSON) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }

    resJSON = JSON.parse(resJSON);
    var iadbSchema = resJSON.dataConnection;
    var findIADB = {
      "properties": [ "name", "imports_database" ],
      "types": [ "data_connection" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "name",
            "operator": "=",
            "value": iadbSchema
          }
        ]
      }
    };

    _createOrUpdateIgnoreList(function(labelRID) {

      igcrest.search(findIADB, function(err, resSearch) {
        if (resSearch.items.length > 0) {
          var item = resSearch.items[0];
          var iadbRID = item.imports_database._id;
          igcrest.update(iadbRID, {'labels': labelRID}, function(err, resUpdate) {
            callback(err, resUpdate);
          });
        } else {
          callback("Unable to find IADB", resSearch);
        }

      });

    });
  
  });

}

/**
 * Create or update an analysis project, to include ALL objects of the specified type known to IGC that were updated after the date received -- necessary before any tasks can be executed
 *
 * @param {string} name - name of the project
 * @param {string} description - description of the project
 * @param {string} type - the type of data the project will handle ["database", "file"]
 * @param {Date} [updatedAfter] - include into the project any objects in IGC last updated after this date
 * @param {requestCallback} callback - callback that handles the response
 */
exports.createOrUpdateAnalysisProject = function(name, description, type, updatedAfter, callback) {

  var schemasDiscovered = [];
  var schemasAdded = [];

  var proj = new Project(name);
  proj.setDescription(description);

  exports.getProjectList(function(err, resList) {

    var bCreate = (resList.indexOf(name) == -1);
    if (bCreate) {
      console.log("Project not found, creating...");
    } else {
      console.log("Project found, updating...");
    }

    exports.getAllItemsToIgnore(function(errIgnore, typesToIgnoreItems) {

      if (type === "database") {
  
        _getAllHostsWithDatabases(function(errHosts, resHosts) {
    
          for (var i = 0; i < resHosts.length; i++) {
            
            var sHostName = resHosts[i];
            if (typesToIgnoreItems.host.indexOf(sHostName) == -1) {

              _getAllDatabasesAndSchemasForHost(sHostName, function(errDBs, resDBs) {
      
                for (var j = 0; j < resDBs.items.length; j++) {
                  var item = resDBs.items[j];
                  var sDbName = item._name;
                  var sHostName = item["host.name"];
                  var aSchemaNames = item["database_schemas.name"];

                  if (typesToIgnoreItems.database.indexOf(sHostName + "::" + sDbName) == -1) {
      
                    for (var k = 0; k < aSchemaNames.length; k++) {
                      var sSchemaName = aSchemaNames[k];

                      if (typesToIgnoreItems.database_schema.indexOf(sHostName + "::" + sDbName + "::" + sSchemaName) == -1) {
                        schemasDiscovered.push(sHostName + "::" + sDbName + "::" + sSchemaName);
          
                        _getAllTablesAndColumnsForSchema(sHostName, sDbName, sSchemaName, updatedAfter, function(errTbls, resTbls) {
          
                          schemasAdded.push(sDbName + "::" + sSchemaName);
                          for (var m = 0; m < resTbls.items.length; m++) {
                            var item = resTbls.items[m];
                            var sTblName = item._name;
                            var aColNames = item["database_columns.name"];
                            var sSchemaName = item["database_schema.name"];
                            var sDbName = item["database_schema.database.name"];
                            var sHostName = item["database_schema.database.host.name"];
                            if (typesToIgnoreItems.database_table.indexOf(sHostName + "::" + sDbName + "::" + sSchemaName + "::" + sTblName) == -1) {
                              proj.addTable(sDbName, sSchemaName, sTblName, aColNames);
                            } else {
                              console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName + "::" + sSchemaName + "::" + sTblName);
                            }
                          }
          
                          if (schemasDiscovered.length == schemasAdded.length) {
                            var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
                            _createOrUpdateProjectRequest(input, bCreate, callback);
                          }
          
                        });
          
                      } else {
                        console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName + "::" + sSchemaName);
                      }
                    }

                  } else {
                    console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName);
                  }
                }
      
              });

            } else {
              console.warn("  ignoring, based on label: " + sHostName);
            }
    
          }
    
        });
      
      } else if (type === "file") {
    
        // TODO: handle file-based analysis
    
      }

    });

  });

}

/**
 * Get a list of Information Analyzer projects
 *
 * @param {listCallback} callback - callback that handles the response
 */
exports.getProjectList = function(callback) {

  this.makeRequest('GET', "/ibm/iis/ia/api/projects", null, function(res, resXML) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    var aNames = [];
    var resDoc = new xmldom.DOMParser().parseFromString(resXML);
    var nlPrj = xpath.select("//*[local-name(.)='Project']", resDoc);
    for (var i = 0; i < nlPrj.length; i++) {
      aNames.push(nlPrj[i].getAttribute("name"));
    }
    callback(err, aNames);
    return aNames;
  });

}

/**
 * Get a list of all of the data sources in the specified Information Analyzer project
 *
 * @param {string} projectName
 * @param {boolean} bByColumn - true iff all detail down to column level is desired; if false will return data store + location and *.* for table / file and column-level detail
 * @param {listCallback} callback - callback that handles the response
 */
exports.getProjectDataSourceList = function(projectName, bByColumn, callback) {

  this.makeRequest('GET', encodeURI("/ibm/iis/ia/api/project?projectName=" + projectName), null, function(res, resXML) {
    
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }

    var aDSs = [];
    var resDoc = new xmldom.DOMParser().parseFromString(resXML);

    var nlDS = xpath.select("//*[local-name(.)='DataSource']", resDoc);
    
    for (var i = 0; i < nlDS.length; i++) {

      var nDataSource = nlDS[i];
      var dataSourceName = nDataSource.getAttribute("name");
      
      var nlSchemas = nDataSource.getElementsByTagName("Schema");
      for (var j = 0; j < nlSchemas.length; j++) {
        var nSchema = nlSchemas.item(j);
        var schemaName = nSchema.getAttribute("name");
        if (!bByColumn) {
          aDSs.push(dataSourceName + "." + schemaName + ".*.*");
        } else {
          var nlTables = nSchema.getElementsByTagName("Table");
          for (var k = 0; k < nlTables.length; k++) {
            var nTable = nlTables.item(k);
            var tableName = nTable.getAttribute("name");
            var nlCols = nTable.getElementsByTagName("Column");
            for (var l = 0; l < nlCols.length; l++) {
              var nColumn = nlCols.item(l);
              var columnName = nColumn.getAttribute("name");
              aDSs.push(dataSourceName + "." + schemaName + "." + tableName + "." + columnName);
            }
          }
        }
      }

      var nlFileFolders = nDataSource.getElementsByTagName("FileFolder");
      for (var j = 0; j < nlFileFolders.length; j++) {
        var nFolder = nlFileFolders.item(j);
        var folderName = nFolder.getAttribute("name");
        if (!bByColumn) {
          aDSs.push(dataSourceName + ":" + folderName + ":*:*");
        } else {
          var nlFiles = nFolder.getElementsByTagName("FileName");
          for (var k = 0; k < nlFiles.length; k++) {
            var nFile = nlFiles.item(k);
            var fileName = nFile.getAttribute("name");
            var nlCols = nFile.getElementsByTagName("Column");
            for (var l = 0; l < nlCols.length; l++) {
              var nColumn = nlCols.item(l);
              var columnName = nColumn.getAttribute("name");
              aDSs.push(dataSourceName + ":" + folderName + ":" + fileName + ":" + columnName);
            }
          }
        }
      }
      
    }

    callback(err, aDSs);

  });

}

/**
 * Run a full column analysis against the data source details specificed
 *
 * @param {string} projectName - name of the IA project
 * @param {string} type - the type of data ["database", "file"]
 * @param {string} [hostname] - hostname of the system containing the data to be analyzed
 * @param {string} datasource - database (type "database") or connection (type "file")
 * @param {string} location - data schema (type "database") or directory path (type "file")
 * @param {requestCallback} callback - callback that handles the response
 */
exports.runColumnAnalysis = function(projectName, type, hostname, datasource, location, callback) {

  var proj = new Project(projectName);
  var ca = new ColumnAnalysis(proj, true, "CAPTURE_ALL", 5000, 10000, true);

  if (type === "database") {
    ca.addColumn(datasource, location, "*", "*", hostname);
  } else if (type === "file") {
    ca.addFileField(datasource, location, "*", "*", hostname);
  }

  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
  exports.makeRequest('POST', "/ibm/iis/ia/api/executeTasks", input, function(res, resExec) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, resExec);
    return resExec;
  });

}

/**
 * Run a full column analysis against the list of data sources specificed
 *
 * @param {string} projectName - name of the IA project
 * @param {string[]} aSources - an array of qualified data source names (DB.SCHEMA.TABLE for databases, HOST:PATH:FILENAME for files)
 * @param {requestCallback} callback - callback that handles the response
 */
exports.runColumnAnalysisForSources = function(projectName, aSources, callback) {

  var proj = new Project(projectName);
  var ca = new ColumnAnalysis(proj, true, "CAPTURE_ALL", 5000, 10000, true);

  for (var i = 0; i < aSources.length; i++) {
    var sSourceName = aSources[i];
    if (sSourceName.indexOf(":") > -1) {
      var aTokens = sSourceName.split(":");
      ca.addFileField(aTokens[0], aTokens[1], aTokens[2], "*");
    } else if (sSourceName.indexOf(".") > -1) {
      var aTokens = sSourceName.split(".");
      ca.addColumn(aTokens[0], aTokens[1], aTokens[2], "*");
    }
  }

  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
  exports.makeRequest('POST', "/ibm/iis/ia/api/executeTasks", input, function(res, resExec) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, resExec);
    return resExec;
  });

}

/**
 * Publish analysis results
 *
 * @param {string} projectName - name of the IA project
 * @param {string} type - the type of data ["database", "file"]
 * @param {string} [hostname] - hostname of the system with analysis results to be published
 * @param {string} datasource - database (type "database") or connection (type "file")
 * @param {string} location - data schema (type "database") or directory path (type "file")
 * @param {requestCallback} callback - callback that handles the response
 */
exports.publishResults = function(projectName, type, hostname, datasource, location, callback) {

  var proj = new Project(projectName);
  var pr = new PublishResults(proj);

  if (type === "database") {
    pr.addTable(datasource, location, "*", hostname);
  } else if (type === "file") {
    pr.addFile(datasource, location, "*", hostname);
  }

  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
  exports.makeRequest('POST', "/ibm/iis/ia/api/publishResults", input, function(res, resExec) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, resExec);
    return resExec;
  });

}

/**
 * Publish analysis results for the list of data sources specificed
 *
 * @param {string} projectName - name of the IA project
 * @param {string[]} aSources - an array of qualified data source names (DB.SCHEMA.TABLE for databases, HOST:PATH:FILENAME for files)
 * @param {requestCallback} callback - callback that handles the response
 */
exports.publishResultsForSources = function(projectName, aSources, callback) {

  var proj = new Project(projectName);
  var pr = new PublishResults(proj);

  for (var i = 0; i < aSources.length; i++) {
    var sSourceName = aSources[i];
    if (sSourceName.indexOf(":") > -1) {
      var aTokens = sSourceName.split(":");
      pr.addFile(aTokens[0], aTokens[1], aTokens[2], null);
    } else if (sSourceName.indexOf(".") > -1) {
      var aTokens = sSourceName.split(".");
      pr.addTable(aTokens[0], aTokens[1], aTokens[2], null);
    }
  }

  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
  exports.makeRequest('POST', "/ibm/iis/ia/api/publishResults", input, function(res, resExec) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, "successful");
    return "successful";
  });

}

/**
 * Retrieve previously published analysis results
 *
 * @param {string} projectName - name of the IA project
 * @param {Date} timeToConsiderStale - the time before which any analysis results should be considered stale
 * @param {requestCallback} callback - callback that handles the response
 */
exports.getStaleAnalysisResults = function(projectName, timeToConsiderStale, callback) {

  // Get a list of all project data sources (everything we should check for staleness)
  exports.getProjectDataSourceList(projectName, true, function (err, aDataSources) {

    var project = new Project(projectName);
    var projectTables = {};
    var projectFiles = {};
    for (var i = 0; i < aDataSources.length; i++) {
      var sDataSource = aDataSources[i];
      if (sDataSource.indexOf(":") > -1) {
        sDataSource = sDataSource.substring(0, sDataSource.lastIndexOf(":"));
        projectFiles[sDataSource] = true;
      } else if (sDataSource.indexOf(".") > -1) {
        sDataSource = sDataSource.substring(0, sDataSource.lastIndexOf("."));
        projectTables[sDataSource] = true;
      }
    }

    // Get a list of objects from IGC that are stale in general (this should be faster at-scale than generally)
    // querying every single one of the data sources above
    // NOTE: it is necessary to do this at table / file level, as the lower level does not allow retrieving contextual information (full identity)
    var staleTables = {
      "pageSize": "1000000",
      "properties": [ "name", "modified_on", "database_table_or_view.name", "database_table_or_view.database_schema.name", "database_table_or_view.database_schema.database.name", "database_table_or_view.database_schema.database.host.name" ],
      "types": [ "table_analysis" ]
    };

    igcrest.search(staleTables, function (err, resSearch) {

      var aTablesToAnalyze = [];

      // Build up a list of any tables with analysis results, so we can compare against project tables  
      var tablesWithAnalysisResults = {};
      for (var i = 0; i < resSearch.items.length; i++) {
        var item = resSearch.items[i];
        var sTblAnalysisName = item._name;
        var sHostName = item["database_table_or_view.database_schema.database.host.name"];
        var sDbName = item["database_table_or_view.database_schema.database.name"];
        var sSchemaName = item["database_table_or_view.database_schema.name"];
        var sTblName = item["database_table_or_view.name"];
        var dModified = new Date(item["modified_on"]);
        var qualifiedName = sDbName + "." + sSchemaName + "." + sTblName;
        if (tablesWithAnalysisResults.hasOwnProperty(qualifiedName)) {
          // Find the most recent analysis result, in case there are multiple
          var lastTime = tablesWithAnalysisResults[qualifiedName];
          if (lastTime < dModified) {
            tablesWithAnalysisResults[qualifiedName] = dModified;
          }
        } else {
          tablesWithAnalysisResults[qualifiedName] = dModified;
        }
      }

      for (var key in projectTables) {
        if (projectTables.hasOwnProperty(key)) {
          // If there is no analysis result, it has never been analyzed -- add it
          if (!tablesWithAnalysisResults.hasOwnProperty(key)) {
            aTablesToAnalyze.push(key);
          } else {
            // Otherwise, check the date of the last analysis
            var lastAnalysis = tablesWithAnalysisResults[key];
            if (lastAnalysis <= timeToConsiderStale) {
              aTablesToAnalyze.push(key);
            }
          }
        }
      }

      callback(err, aTablesToAnalyze);

    });

// TODO: handle files
/*
    var staleFileFields = {
      "pageSize": "10000",
      "properties": [ "name", "data_file_record.name", "data_file_record.data_file.path", "data_file_record.data_file.host.name" ],
      "types": [ "file_record_analysis" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "modified_on",
            "operator": "<=",
            "value": timeToConsiderStale.valueOf()
          }
        ]
      }
    };

    console.log("Searching fields: " + JSON.stringify(staleFileFields));
    igcrest.search(staleFileFields, function (err, resSearch) {
  
      console.log(JSON.stringify(resSearch));
      callback(err, resSearch);

    });
*/
    // Compare the two lists -- any overlap will be our results

  });

}

/**
 * Get the status of a running task
 *
 * @param {string} executionID - the unique identification number of the running task
 * @param {statusCallback} callback - callback that handles the response
 */
exports.getTaskStatus = function(executionID, callback) {
  this.makeRequest('GET', "/ibm/iis/ia/api/analysisStatus?scheduleID=" + executionID, null, function(res, resStatus) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    var stat = {};
    var resDoc = new xmldom.DOMParser().parseFromString(resStatus);
    var nlExec = xpath.select("//*[local-name(.)='TaskExecution']", resDoc);
    if (nlExec.length > 1) {
      err = "More than one result found";
    }
    for (var i = 0; i < nlExec.length; i++) {
      stat.executionId = nlExec[i].getAttribute("executionId");
      stat.executionTime = nlExec[i].getAttribute("executionTime");
      stat.progress = nlExec[i].getAttribute("progress");
      stat.status = nlExec[i].getAttribute("status");
    }
    callback(err, stat);
    return stat;
  });
}

/**
 * Retrieves any execution IDs from the provided response
 *
 * @param {string} resXML
 * @returns {string[]} an array of execution IDs
 */
exports.getExecutionIDsFromResponse = function(resXML) {

  var executionIDs = [];
  var resDoc = new xmldom.DOMParser().parseFromString(resXML);
  var nlTask = xpath.select("//*[local-name(.)='ScheduledTask']", resDoc);
  for (var i = 0; i < nlTask.length; i++) {
    var executionID = nlTask[i].getAttribute("scheduleId");
    executionIDs.push(executionID);
  }
  return executionIDs;

}

/**
 * Issues a request to reindex Solr for any resutls to appear appropriately in the IA Thin Client
 *
 * @param {int} batchSize - The batch size to retrieve information from the database. Increasing this size may improve performance but there is a possibility of reindex failure. The default is 25. The maximum value is 1000.
 * @param {int} solrBatchSize - The batch size to use for Solr indexing. Increasing this size may improve performance. The default is 100. The maximum value is 1000.
 * @param {boolean} upgrade - Specifies whether to upgrade the index schema from a previous version, and is a one time requirement when upgrading from one version of the thin client to another. The schema upgrade can be used to upgrade from any previous version of the thin client. The value true will upgrade the index schema. The value false is the default, and will not upgrade the index schema.
 * @param {boolean} force - Specifies whether to force reindexing if indexing is already in process. The value true will force a reindex even if indexing is in process. The value false is the default, and prevents a reindex if indexing is already in progress. This option should be used if a previous reindex request is aborted for any reason. For example, if InfoSphere Information Server services tier system went offline, you would use this option.
 * @param {reindexCallback} callback - status of the reindex ["REINDEX_SUCCESSFUL"]
 */
exports.reindexThinClient = function(batchSize, solrBatchSize, upgrade, force, callback) {
  var request = "/ibm/iis/dq/da/rest/v1/reindex";
  request = request
            + "?batchSize=" + _getValueOrDefault(batchSize, 25)
            + "&solrBatchSize=" + _getValueOrDefault(solrBatchSize, 100)
            + "&upgrade=" + _getValueOrDefault(upgrade, false)
            + "&force=" + _getValueOrDefault(force, true);
  this.makeRequest('GET', request, null, function(res, resStatus) {
    var err = null;
    if (res.statusCode != 200) {
      err = "Unsuccessful request " + res.statusCode;
      console.error(err);
      console.error('headers: ', res.headers);
      throw new Error(err);
    }
    callback(err, resStatus);
    return resStatus;
  });
}

/**
 * This callback is invoked as the result of an IA REST API call, providing the response of that request.
 * @callback requestCallback
 * @param {string} errorMessage - any error message, or null if no errors
 * @param {string} responseXML - the XML of the response
 */

/**
 * This callback is invoked as the result of an IA REST API call, providing the response of that request.
 * @callback listCallback
 * @param {string} errorMessage - any error message, or null if no errors
 * @param {string[]} aResponse - the response of the request, in the form of an array
 */

/**
 * This callback is invoked as the result of an IA REST API call, providing the response of that request.
 * @callback statusCallback
 * @param {string} errorMessage - any error message, or null if no errors
 * @param {Object} status - the response of the request, in the form of an object keyed by execution ID, with subkeys for executionTime, progress and status ["running", "successful", "failed", "cancelled"]
 */

/**
 * This callback is invoked as the result of an IA REST API call to re-index Solr for IATC
 * @callback reindexCallback
 * @param {string} errorMessage - any error message, or null if no errors
 * @param {string} status - the status of the reindex operation ["REINDEX_SUCCESSFUL"]
 */

 /**
  * This callback is invoked as the result of retrieving a list of items that Information Analyzer should ignore
  * @callback itemsToIgnoreCallback
  * @param {string} errorMessage - any error message, or null if no errors
  * @param {Object} typeToIdentities - dictionary keyed by object type, with each value being an array of objects of that type to ignore (as identity strings, /-delimited)
  */
