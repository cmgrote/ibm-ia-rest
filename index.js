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
 * // executes a column analysis against the schema "TESTSCH", in the database "TESTDB", on the host "REPO"
 * var iarest = require('ibm-ia-rest');
 * iarest.setAuth("isadmin", "isadmin");
 * iarest.setServer("hostname", "9445");
 * iarest.runFullColumnAnalysis("database", "REPO", "TESTDB", "TESTSCH", function(err, results) {
 *   // TODO: demonstrate get the executionID out of the results
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
      name = hostname + "." + name;
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
      name = hostname + ":" + name;
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
        }
      ]
    }
  }

  igcrest.search(json, function (err, resSearch) {

    callback(err, resSearch);

  });

}

/**
 * @private
 */
function _getAllTablesAndColumnsForSchema(hostname, datasource, schema, callback) {

  var json = {
    "pageSize": "10000",
    "properties": [ "name", "database_columns.name", "database_schema.name", "database_schema.database.name" ],
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
        }
      ]
    }
  }

  igcrest.search(json, function (err, resSearch) {
  
    callback(err, resSearch);

  });

}

/**
 * @private
 */
function _createProjectRequest(inputXML, callback) {
  exports.makeRequest('POST', "/ibm/iis/ia/api/create", inputXML, function(res, resCreate) {
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
 * Create an analysis project -- necessary before any tasks can be executed
 *
 * @param {string} name - name of the project
 * @param {string} description - description of the project
 * @param {string} type - the type of data the project will handle ["database", "file"]
 * @param {requestCallback} callback - callback that handles the response
 */
exports.createAnalysisProject = function(name, description, type, callback) {

  var schemasDiscovered = [];
  var schemasAdded = [];

  var proj = new Project(name);
  proj.setDescription(description);

  if (type === "database") {

    _getAllHostsWithDatabases(function(errHosts, resHosts) {

      for (var i = 0; i < resHosts.length; i++) {
        
        var sHostName = resHosts[i];
        _getAllDatabasesAndSchemasForHost(sHostName, function(errDBs, resDBs) {

          for (var j = 0; j < resDBs.items.length; j++) {
            var item = resDBs.items[j];
            var sDbName = item._name;
            var sHostName = item["host.name"];
            var aSchemaNames = item["database_schemas.name"];

            for (var k = 0; k < aSchemaNames.length; k++) {
              var sSchemaName = aSchemaNames[k];
              schemasDiscovered.push(sHostName + "/" + sDbName + "/" + sSchemaName);

              _getAllTablesAndColumnsForSchema(sHostName, sDbName, sSchemaName, function(errTbls, resTbls) {

                schemasAdded.push(sDbName + "/" + sSchemaName);
                for (var m = 0; m < resTbls.items.length; m++) {
                  var item = resTbls.items[m];
                  var sTblName = item._name;
                  var aColNames = item["database_columns.name"];
                  var sSchemaName = item["database_schema.name"];
                  var sDbName = item["database_schema.database.name"];
                  proj.addTable(sDbName, sSchemaName, sTblName, aColNames);
                }

                if (schemasDiscovered.length == schemasAdded.length) {
                  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
                  _createProjectRequest(input, callback);
                }

              });

            }
          }

        });

      }

    });
  
  } else if (type === "file") {

    // TODO: handle file-based analysis

  }

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
 * Run a full column analysis against the data source details specificed
 *
 * @param {string} projectName - name of the IA project
 * @param {string} type - the type of data ["database", "file"]
 * @param {string} hostname - hostname of the system containing the data to be analyzed
 * @param {string} datasource - database (type "database") or connection (type "file")
 * @param {string} location - data schema (type "database") or directory path (type "file")
 * @param {requestCallback} callback - callback that handles the response
 */
exports.runColumnAnalysis = function(projectName, type, hostname, datasource, location, callback) {

  var proj = new Project(projectName);
  var ca = new ColumnAnalysis(proj, true, "CAPTURE_ALL", 5000, 10000, true);

  if (type === "database") {
    ca.addColumn(datasource, location, "*", "*", hostname.toUpperCase());
  } else if (type === "file") {
    ca.addFileField(datasource, location, "*", "*", hostname.toUpperCase());
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
 * @param {string} hostname - hostname of the system with analysis results to be published
 * @param {string} datasource - database (type "database") or connection (type "file")
 * @param {string} location - data schema (type "database") or directory path (type "file")
 * @param {requestCallback} callback - callback that handles the response
 */
exports.publishResults = function(projectName, type, hostname, datasource, location, callback) {

  var proj = new Project(projectName);
  var pr = new PublishResults(proj);

  if (type === "database") {
    pr.addTable(datasource, location, "*", hostname.toUpperCase());
  } else if (type === "file") {
    pr.addFile(datasource, location, "*", hostname.toUpperCase());
  }

  var input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
  console.log(input);
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
            + "batchSize=" + _getValueOrDefault(batchSize, 25)
            + "solrBatchSize=" + _getValueOrDefault(solrBatchSize, 100)
            + "upgrade=" + _getValueOrDefault(upgrade, false)
            + "force=" + _getValueOrDefault(force, true);
            console.log(request);
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
