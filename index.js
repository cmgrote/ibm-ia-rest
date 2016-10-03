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

"use strict";

/**
 * @file Re-usable functions for interacting with IA's REST API
 * @license Apache-2.0
 * @requires xmldom
 * @requires xpath
 * @requires ibm-igc-rest
 * @requires ibm-iis-commons
 * @example
 * // runs column analysis for any objects in Automated Profiling that have not been analyzed since the moment the script is run (new Date())
 * var iarest = require('ibm-ia-rest');
 * var commons = require('ibm-iis-commons');
 * var restConnect = new commons.RestConnection("isadmin", "isadmin", "hostname", "9445");
 * iarest.setConnection(restConnect);
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
const igcrest = require('ibm-igc-rest');

const Project = require('./classes/project');
const ColumnAnalysis = require('./classes/column-analysis');
const PublishResults = require('./classes/publish-results');

const RestIA = (function() {

  const ignoreLabelName = "Information Analyzer Ignore List";
  const ignoreLabelDesc = "Information Analyzer should ignore any assets with this label; they should not be indexed.";
  
  let _restConnect = null;
  
  /**
   * Set the connection for the REST API
   * 
   * @param {RestConnection} restConnect - RestConnection object, from ibm-iis-commons
   */
  const setConnection = function(restConnect) {
    _restConnect = restConnect;
    igcrest.setConnection(restConnect);
  };
  
  /**
   * @private
   */
  function _getValueOrDefault(val, def) {
    return (val === undefined) ? def : val;
  }
  
  /**
   * Make a request against IA's REST API
   *
   * @see module:ibm-ia-rest.setServer
   * @see module:ibm-ia-rest.setAuth
   * @param {string} method - type of request, one of ['GET', 'PUT', 'POST', 'DELETE']
   * @param {string} path - the path to the end-point (e.g. /ibm/iis/ia/api/...)
   * @param {string} [input] - any input for the request, i.e. for PUT, POST
   * @param {string} [inputType] - the type of input, if any provided ['text/xml', 'application/json']
   * @param {requestCallback} callback - callback that handles the response
   * @throws will throw an error if connectivity details are incomplete or there is a fatal error during the request
   */
  const makeRequest = function(method, path, input, inputType, callback) {
  
    const bInput = (typeof input !== 'undefined' && input !== null);
    input = (inputType === 'application/json' ? JSON.stringify(input) : input);
    
    if (typeof _restConnect === 'undefined' || _restConnect === undefined || _restConnect === null) {
      throw new Error("Setup incomplete: no connection found.");
    }
  
    const opts = {
      auth: _restConnect.auth,
      hostname: _restConnect.host,
      port: _restConnect.port,
      path: path,
      method: method,
      rejectUnauthorized: false,
      maxSockets: 1,
      keepAlive: false
    };
    if (bInput) {
      opts.headers = {
        'Content-Type': inputType,
        'Content-Length': input.length
      };
    }
    opts.agent = new https.Agent(opts);
  
    const req = https.request(opts, (res) => {
  
      let data = "";
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
  
  };
  
  /**
   * @private
   */
  function _getAllHostsWithDatabases(callback) {
    
    const json = {
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
    };
  
    igcrest.search(json, function (err, resSearch) {
  
      const toReturn = [];
      for (let i = 0; i < resSearch.items.length; i++) {
        const item = resSearch.items[i];
        const sHostName = item._name;
        toReturn.push(sHostName);
      }
      callback(err, toReturn);
  
    });
  
  }
  
  /**
   * @private
   */
  function _getAllHostsWithFiles(callback) {
  
    const json = {
      "pageSize": "10000",
      "properties": [ "name" ],
      "types": [ "host" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "data_file_folders",
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
    };
  
    igcrest.search(json, function (err, resSearch) {
  
      const toReturn = [];
      for (let i = 0; i < resSearch.items.length; i++) {
        const item = resSearch.items[i];
        const sHostName = item._name;
        toReturn.push(sHostName);
      }
      callback(err, toReturn);
  
    });
  
  }
  
  function _getLocalFileConnectorForHost(hostname, callback) {
  
    const json = {
      "pageSize": "10000",
      "properties": [ "name", "data_connectors.type" ],
      "types": [ "data_connection" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "data_connectors.host.name",
            "operator": "=",
            "value": hostname
          },
          {
            "property": "data_connectors.type",
            "operator": "=",
            "value": "LocalFileConnector"
          }
        ]
      }
    };
  
    igcrest.search(json, function (err, resSearch) {
  
      let dcnRID = "";
      if (resSearch.items.length > 0) {
        const takeTheFirst = resSearch.items[0];
        dcnRID = takeTheFirst._id;
      }
      callback(err, dcnRID);
      return dcnRID;
  
    });
  
  }
  
  /**
   * @private
   */
  function _getAllFoldersAndFilesForHost(hostname, typesToIgnoreItems, bCreate, callback) {
  
    const json = {
      "pageSize": "10000",
      "properties": [ "name", "data_files", "host.name" ],
      "types": [ "data_file_folder" ],
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
  
      callback(err, resSearch, bCreate, typesToIgnoreItems);
  
    });
  
  }
  
  /**
   * @private
   */
  function _getAllFieldNamesForFile(fileRID, updatedAfter, bCreate, callback) {
  
    const json = {
      "pageSize": "10000",
      "properties": [ "name", "data_file_fields" ],
      "types": [ "data_file_record" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "data_file",
            "operator": "=",
            "value": fileRID
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
  
    if (updatedAfter !== null) {
      json.where.conditions.push({
        "property": "modified_on",
        "operator": ">=",
        "value": updatedAfter.valueOf()
      });
    }
  
    igcrest.search(json, function (err, resSearch) {
      const fieldList = {};
      for (let i = 0; i < resSearch.items.length; i++) {
        const item = resSearch.items[i];
        fieldList.id = igcrest.getItemIdentityString(item);
        fieldList.fields = [];
        const fields = item.data_file_fields.items;
        for (let j = 0; j < fields.length; j++) {
          const fieldName = fields[j]._name;
          fieldList.fields.push(fieldName);
        }
      }
      callback(err, fieldList, bCreate);
    });
  
  }
  
  /**
   * @private
   */
  function _getAllDatabasesAndSchemasForHost(hostname, typesToIgnoreItems, bCreate, callback) {
  
    const json = {
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
  
      callback(err, resSearch, bCreate, typesToIgnoreItems);
  
    });
  
  }
  
  /**
   * @private
  function _getAllSchemasForDatabase(hostname, datasource, callback) {
  
    const json = {
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
   */
  
  /**
   * @private
   */
  function _getAllTablesAndColumnsForSchema(hostname, datasource, schema, updatedAfter, typesToIgnoreItems, bCreate, callback) {
  
    const json = {
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
  
    if (updatedAfter !== null) {
      json.where.conditions.push({
        "property": "modified_on",
        "operator": ">=",
        "value": updatedAfter.valueOf()
      });
    }
  
    igcrest.search(json, function (err, resSearch) {
    
      callback(err, resSearch, bCreate, typesToIgnoreItems, datasource, schema);
  
    });
  
  }
  
  /**
   * Retrieves a list of all items that should be ignored, i.e. where they are labelled with "Information Analyzer Ignore List"
   *
   * @param {itemsToIgnoreCallback} callback
   */
  const getAllItemsToIgnore = function(callback) {
  
    // NOTE: the query below looks backwards with 'negated=false', but unfortunately only seems to work this way
    const json = {
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
      const typesToItems = {
        "host": [],
        "database": [],
        "database_schema": [],
        "database_table": [],
        "database_column": [],
        "data_file_folder": [],
        "data_file": []
      };
      for (let i = 0; i < resSearch.items.length; i++) {
        const item = resSearch.items[i];
        const type = item._type;
        typesToItems[type].push(igcrest.getItemIdentityString(item));
      }
      callback(err, typesToItems);
    });
  
  };
  
  /**
   * @private
   */
  function _createOrUpdateProjectRequest(inputXML, bCreate, callback) {
    const endpoint = (bCreate) ? "/ibm/iis/ia/api/create" : "/ibm/iis/ia/api/update";
    makeRequest('POST', endpoint, inputXML, 'text/xml', function(res, resCreate) {
      let err = null;
      if (res.statusCode !== 200) {
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
  function _prepFilesArray(aFiles) {
  
    let fileList = "";
    for (let i = 0; i < aFiles.length; i++) {
      const file = aFiles[i];
      if (file.endsWith("/")) {
        fileList = fileList + ";folder[" + file + "]";
      } else {
        fileList = fileList + ";file[" + file + "]";
      }
    }
    if (fileList.length > 0) {
      fileList = fileList.substring(1);
    }
    return fileList;
  
  }
  
  /**
   * @private
   */
  // NOTE: this uses an internal / unpublished "DA REST API" -- subject to change without notice...
  // (discovered using Firebug to look at all communication in the IATC)
  function _addFilesToProject(dcnRID, projectRID, aFileList, callback) {
    
    const getHostnameJSON = {
      "pageSize": "5",
      "properties": [ "name", "data_connectors.host.name" ],
      "types": [ "data_connection" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "_id",
            "operator": "=",
            "value": dcnRID
          }
        ]
      }
    };
  
    igcrest.search(getHostnameJSON, function (err, resSearch) {
  
      if (resSearch.items.length > 0) {
        const sHostName = resSearch.items[0]["data_connectors.host.name"];
  
        const fileList = _prepFilesArray(aFileList);
        const endpoint = "/ibm/iis/dq/da/rest/v1/catalog/dataSets/doRegisterAndAddToWorkspaces";
        const addFilesJSON = {
          "properties": {
            "registrationParameters": {
              "Identity_HostSystem": sHostName,
              "DataConnection": dcnRID,
              "ImportFileStructure": "true",
              "IgnoreAccessError": "false",
              "DirectoryContents": fileList
            },
            "connectionParameters": {},
            "formattingParameters": "delim=',',header='true',charset='UTF-8'"
          },
          "options": {
            "workspaces": [
            {
              "rid": projectRID
            }]
          }
        };
  
        //exports.makeRequest('POST', endpoint, addFilesJSON, 'application/json', function(res, resUpdate) {
        makeRequest('POST', endpoint, addFilesJSON, 'application/json', function(res) {
          let err = null;
          if (res.statusCode !== 200) {
            err = "Unsuccessful request " + res.statusCode;
            console.error(err);
            console.error("headers: ", res.headers);
            throw new Error(err);
          }
          callback(err, "successful");
          return "successful";
        });
  
      }
  
    });
  
    
  
  }
  
  /**
   * @private
   */
  function _getProjectRID(projectDescription, callback) {
    const json = {
      "pageSize": "5",
      "properties": [ "short_description" ],
      "types": [ "analysis_project" ],
      "where":
      {
        "operator": "and",
        "conditions":
        [
          {
            "property": "short_description",
            "operator": "=",
            "value": projectDescription
          }
        ]
      }
    };
    igcrest.search(json, function (err, resSearch) {
      let projectRID = "";
      if (resSearch.items.length > 0) {
        projectRID = resSearch.items[0]._id;
      }
      callback(err, projectRID);
      return projectRID;
    });
  }
  
  /**
   * @private
   */
  function _createOrUpdateIgnoreList(callback) {
  
    //var labelName = "Information Analyzer Ignore List";
    //var labelDesc = "Information Analyzer should ignore any assets with this label; they should not be indexed.";
    const queryLabelExistence = {
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
    
      let labelRID = "";
      for (let i = 0; i < resSearch.items.length; i++) {
        const item = resSearch.items[i];
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
  const addIADBToIgnoreList = function(callback) {
    
    makeRequest('GET', "/ibm/iis/ia/api/getIADBParams", null, null, function(res, resJSON) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
  
      resJSON = JSON.parse(resJSON);
      const iadbSchema = resJSON.dataConnection;
      const findIADB = {
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
            const item = resSearch.items[0];
            const iadbRID = item.imports_database._id;
            igcrest.update(iadbRID, {'labels': labelRID}, function(err, resUpdate) {
              callback(err, resUpdate);
            });
          } else {
            callback("Unable to find IADB", resSearch);
          }
  
        });
  
      });
    
    });
  
  };
  
  /**
   * Create or update an analysis project, to include ALL objects known to IGC that were updated after the date received -- necessary before any tasks can be executed
   *
   * @param {string} name - name of the project
   * @param {string} description - description of the project
   * @param {Date} [updatedAfter] - include into the project any objects in IGC last updated after this date
   * @param {requestCallback} callback - callback that handles the response
   */
  const createOrUpdateAnalysisProject = function(name, description, updatedAfter, callback) {
  
    const schemasDiscovered = [];
    const schemasAdded = [];
    const aFileList = [];
  
    const proj = new Project(name);
    proj.setDescription(description);
  
    function processTablesAndColumns(errTbls, resTbls, bCreate, typesToIgnoreItems, sDbName, sSchemaName) {
    
      schemasAdded.push(sDbName + "::" + sSchemaName);
      let sHostName = "";
      for (let m = 0; m < resTbls.items.length; m++) {
        const item = resTbls.items[m];
        const sTblName = item._name;
        const aColNames = item["database_columns.name"];
        const sSchemaName = item["database_schema.name"];
        const sDbName = item["database_schema.database.name"];
        sHostName = item["database_schema.database.host.name"];
        if (typesToIgnoreItems.database_table.indexOf(sHostName + "::" + sDbName + "::" + sSchemaName + "::" + sTblName) === -1) {
          proj.addTable(sDbName, sSchemaName, sTblName, aColNames);
        } else {
          console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName + "::" + sSchemaName + "::" + sTblName);
        }
      }
    
      if (schemasDiscovered.length === schemasAdded.length) {
        const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
        //console.log("Input (dbs): " + input);
        _createOrUpdateProjectRequest(input, bCreate, function() {
          _getLocalFileConnectorForHost(sHostName, function(errDCN, dcnRID) {
            _getProjectRID(description, function(errPrj, projectRID) {
              _addFilesToProject(dcnRID, projectRID, aFileList, callback);
            });
          });
        });
      }
    
    }
  
    function processDBsAndSchemas(errDBs, resDBs, bCreate, typesToIgnoreItems) {
        
      for (let j = 0; j < resDBs.items.length; j++) {
        const item = resDBs.items[j];
        const sDbName = item._name;
        const sHostName = item["host.name"];
        const aSchemaNames = item["database_schemas.name"];
  
        if (typesToIgnoreItems.database.indexOf(sHostName + "::" + sDbName) === -1) {
    
          for (let k = 0; k < aSchemaNames.length; k++) {
            const sSchemaName = aSchemaNames[k];
  
            if (typesToIgnoreItems.database_schema.indexOf(sHostName + "::" + sDbName + "::" + sSchemaName) === -1) {
              schemasDiscovered.push(sHostName + "::" + sDbName + "::" + sSchemaName);
  
              _getAllTablesAndColumnsForSchema(sHostName, sDbName, sSchemaName, updatedAfter, typesToIgnoreItems, bCreate, processTablesAndColumns);
    
            } else {
              console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName + "::" + sSchemaName);
            }
          }
  
        } else {
          console.warn("  ignoring, based on label: " + sHostName + "::" + sDbName);
        }
      }
    
    }
  
    function processFieldNames(err, fieldList, bCreate) {
      if (fieldList.hasOwnProperty("id")) { // only proceed if there were actually any files that met the criteria
        const identity = fieldList.id;
        schemasAdded.push(identity);
        const sHostName = identity.substring(0, identity.indexOf("::"));
        //const sFileRecord = identity.substring(identity.lastIndexOf("::") + 2);
        let sFolderPath = identity.substring(identity.indexOf("::") + 2, identity.lastIndexOf("::"));
        const fileName = sFolderPath.substring(sFolderPath.lastIndexOf("::") + 2);
        sFolderPath = sFolderPath.substring(0, sFolderPath.lastIndexOf("::")).replace(new RegExp("::", 'g'), "/");
        if (sFolderPath.startsWith("//")) {
          sFolderPath = sFolderPath.substring(1);
        }
        //proj.addFile(sHostName, sFolderPath, fileName, fieldList.fields);
        aFileList.push(sFolderPath + "/" + fileName);
    
        if (schemasDiscovered.length === schemasAdded.length) {
          const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
          //console.log("Input (files): " + input);
          _createOrUpdateProjectRequest(input, bCreate, function() {
            _getLocalFileConnectorForHost(sHostName, function(errDCN, dcnRID) {
              _getProjectRID(description, function(errPrj, projectRID) {
                _addFilesToProject(dcnRID, projectRID, aFileList, callback);
              });
            });
          });
        }
      }
    }
  
    function processFilesAndFolders(errFiles, resFolders, bCreate, typesToIgnoreItems) {
  
      //const foldersToFiles = {};
      for (let j = 0; j < resFolders.items.length; j++) {
        const item = resFolders.items[j];
        const folderPath = igcrest.getItemIdentityString(item);
  
        if (typesToIgnoreItems.data_file_folder.indexOf(folderPath) === -1) {
  
          const aFiles = item.data_files.items;
          for (let k = 0; k < aFiles.length; k++) {
            const fileItem = aFiles[k];
            const fileRID = fileItem._id;
            const fileName = fileItem._name;
  
            if (typesToIgnoreItems.data_file.indexOf(folderPath + "::" + fileName) === -1) {
  
              schemasDiscovered.push(folderPath + "::" + fileName);
              //const sHostName = folderPath.substring(0, folderPath.indexOf("::"));
              //const sFolderPath = folderPath.substring(folderPath.indexOf("::") + 2);
  
              _getAllFieldNamesForFile(fileRID, updatedAfter, bCreate, processFieldNames);
  
            } else {
              console.warn("  ignoring, based on label: " + folderPath + "::" + fileName);
            }
          }
  
        } else {
          console.warn("  ignoring, based on label: " + folderPath);
        }
      }
  
    }
  
    getProjectList(function(err, resList) {
  
      const bCreate = (resList.indexOf(name) === -1);
      if (bCreate) {
        console.log("Project not found, creating...");
      } else {
        console.log("Project found, updating...");
      }
  
      getAllItemsToIgnore(function(errIgnore, typesToIgnoreItems) {
  
        _getAllHostsWithDatabases(function(errHosts, resHosts) {
          for (let i = 0; i < resHosts.length; i++) {          
            const sHostName = resHosts[i];
            if (typesToIgnoreItems.host.indexOf(sHostName) === -1) {
              _getAllDatabasesAndSchemasForHost(sHostName, typesToIgnoreItems, bCreate, processDBsAndSchemas);
            } else {
              console.warn("  ignoring, based on label: " + sHostName);
            }
      
          }
        });
  
        _getAllHostsWithFiles(function(errHosts, resHosts) {
          for (let i = 0; i < resHosts.length; i++) {
            const sHostName = resHosts[i];
            if (typesToIgnoreItems.host.indexOf(sHostName) === -1) {
              _getAllFoldersAndFilesForHost(sHostName, typesToIgnoreItems, bCreate, processFilesAndFolders);
            } else {
              console.warn("  ignoring, based on label: " + sHostName);
            }
          }
        });
  
      });
  
    });
  
  };
  
  /**
   * Get a list of Information Analyzer projects
   *
   * @param {listCallback} callback - callback that handles the response
   */
  const getProjectList = function(callback) {
  
    makeRequest('GET', "/ibm/iis/ia/api/projects", null, null, function(res, resXML) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      const aNames = [];
      const resDoc = new xmldom.DOMParser().parseFromString(resXML);
      const nlPrj = xpath.select("//*[local-name(.)='Project']", resDoc);
      for (let i = 0; i < nlPrj.length; i++) {
        aNames.push(nlPrj[i].getAttribute("name"));
      }
      callback(err, aNames);
      return aNames;
    });
  
  };
  
  /**
   * Get a list of all of the data sources in the specified Information Analyzer project
   *
   * @param {string} projectName
   * @param {boolean} bByColumn - true iff all detail down to column level is desired; if false will return data store + location and *.* for table / file and column-level detail
   * @param {listCallback} callback - callback that handles the response
   */
  const getProjectDataSourceList = function(projectName, bByColumn, callback) {
  
    makeRequest('GET', encodeURI("/ibm/iis/ia/api/project?projectName=" + projectName), null, null, function(res, resXML) {
      
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
  
      const aDSs = [];
      const resDoc = new xmldom.DOMParser().parseFromString(resXML);
  
      const nlDS = xpath.select("//*[local-name(.)='DataSource']", resDoc);
      
      for (let i = 0; i < nlDS.length; i++) {
  
        const nDataSource = nlDS[i];
        const dataSourceName = nDataSource.getAttribute("name");
        
        const nlSchemas = nDataSource.getElementsByTagName("Schema");
        for (let j = 0; j < nlSchemas.length; j++) {
          const nSchema = nlSchemas.item(j);
          const schemaName = nSchema.getAttribute("name");
          if (!bByColumn) {
            aDSs.push(dataSourceName + "." + schemaName + ".*.*");
          } else {
            const nlTables = nSchema.getElementsByTagName("Table");
            for (let k = 0; k < nlTables.length; k++) {
              const nTable = nlTables.item(k);
              const tableName = nTable.getAttribute("name");
              const nlCols = nTable.getElementsByTagName("Column");
              for (let l = 0; l < nlCols.length; l++) {
                const nColumn = nlCols.item(l);
                const columnName = nColumn.getAttribute("name");
                aDSs.push(dataSourceName + "." + schemaName + "." + tableName + "." + columnName);
              }
            }
          }
        }
  
        const nlFileFolders = nDataSource.getElementsByTagName("FileFolder");
        for (let j = 0; j < nlFileFolders.length; j++) {
          const nFolder = nlFileFolders.item(j);
          const folderName = nFolder.getAttribute("name");
          if (!bByColumn) {
            aDSs.push(dataSourceName + ":" + folderName + ":*:*");
          } else {
            const nlFiles = nFolder.getElementsByTagName("FileName");
            for (let k = 0; k < nlFiles.length; k++) {
              const nFile = nlFiles.item(k);
              const fileName = nFile.getAttribute("name");
              const nlCols = nFile.getElementsByTagName("Column");
              for (let l = 0; l < nlCols.length; l++) {
                const nColumn = nlCols.item(l);
                const columnName = nColumn.getAttribute("name");
                aDSs.push(dataSourceName + ":" + folderName + ":" + fileName + ":" + columnName);
              }
            }
          }
        }
        
      }
  
      callback(err, aDSs);
  
    });
  
  };
  
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
  const runColumnAnalysis = function(projectName, type, hostname, datasource, location, callback) {
  
    const proj = new Project(projectName);
    const ca = new ColumnAnalysis(proj, true, "CAPTURE_ALL", 5000, 10000, true);
  
    if (type === "database") {
      ca.addColumn(datasource, location, "*", "*", hostname);
    } else if (type === "file") {
      ca.addFileField(datasource, location, "*", "*", hostname);
    }
  
    const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
    makeRequest('POST', "/ibm/iis/ia/api/executeTasks", input, 'text/xml', function(res, resExec) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      callback(err, resExec);
      return resExec;
    });
  
  };
  
  /**
   * Run a full column analysis against the list of data sources specificed
   *
   * @param {string} projectName - name of the IA project
   * @param {string[]} aSources - an array of qualified data source names (DB.SCHEMA.TABLE for databases, HOST:PATH:FILENAME for files)
   * @param {requestCallback} callback - callback that handles the response
   */
  const runColumnAnalysisForSources = function(projectName, aSources, callback) {
  
    const proj = new Project(projectName);
    const ca = new ColumnAnalysis(proj, true, "CAPTURE_ALL", 5000, 10000, true);
  
    for (let i = 0; i < aSources.length; i++) {
      const sSourceName = aSources[i];
      if (sSourceName.indexOf(":") > -1) {
        const aTokens = sSourceName.split(":");
        ca.addFileField(aTokens[0], aTokens[1], aTokens[2], "*");
      } else if (sSourceName.indexOf(".") > -1) {
        const aTokens = sSourceName.split(".");
        ca.addColumn(aTokens[0], aTokens[1], aTokens[2], "*");
      }
    }
  
    const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
    makeRequest('POST', "/ibm/iis/ia/api/executeTasks", input, 'text/xml', function(res, resExec) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      callback(err, resExec);
      return resExec;
    });
  
  };
  
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
  const publishResults = function(projectName, type, hostname, datasource, location, callback) {
  
    const proj = new Project(projectName);
    const pr = new PublishResults(proj);
  
    if (type === "database") {
      pr.addTable(datasource, location, "*", hostname);
    } else if (type === "file") {
      pr.addFile(datasource, location, "*", hostname);
    }
  
    const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
    makeRequest('POST', "/ibm/iis/ia/api/publishResults", input, 'text/xml', function(res, resExec) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      callback(err, resExec);
      return resExec;
    });
  
  };
  
  /**
   * Publish analysis results for the list of data sources specificed
   *
   * @param {string} projectName - name of the IA project
   * @param {string[]} aSources - an array of qualified data source names (DB.SCHEMA.TABLE for databases, HOST:PATH:FILENAME for files)
   * @param {requestCallback} callback - callback that handles the response
   */
  const publishResultsForSources = function(projectName, aSources, callback) {
  
    const proj = new Project(projectName);
    const pr = new PublishResults(proj);
  
    for (let i = 0; i < aSources.length; i++) {
      const sSourceName = aSources[i];
      if (sSourceName.indexOf(":") > -1) {
        const aTokens = sSourceName.split(":");
        pr.addFile(aTokens[0], aTokens[1], aTokens[2], null);
      } else if (sSourceName.indexOf(".") > -1) {
        const aTokens = sSourceName.split(".");
        pr.addTable(aTokens[0], aTokens[1], aTokens[2], null);
      }
    }
  
    const input = new xmldom.XMLSerializer().serializeToString(proj.getProjectDoc());
    //exports.makeRequest('POST', "/ibm/iis/ia/api/publishResults", input, 'text/xml', function(res, resExec) {
    makeRequest('POST', "/ibm/iis/ia/api/publishResults", input, 'text/xml', function(res) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      callback(err, "successful");
      return "successful";
    });
  
  };
  
  /**
   * Retrieve previously published analysis results
   *
   * @param {string} projectName - name of the IA project
   * @param {Date} timeToConsiderStale - the time before which any analysis results should be considered stale
   * @param {requestCallback} callback - callback that handles the response
   */
  const getStaleAnalysisResults = function(projectName, timeToConsiderStale, callback) {
  
    // Get a list of all project data sources (everything we should check for staleness)
    getProjectDataSourceList(projectName, true, function (err, aDataSources) {
  
      //const project = new Project(projectName);
      const projectTables = {};
      const projectFiles = {};
      for (let i = 0; i < aDataSources.length; i++) {
        let sDataSource = aDataSources[i];
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
      const staleTables = {
        "pageSize": "1000000",
        "properties": [ "name", "modified_on", "database_table_or_view.name", "database_table_or_view.database_schema.name", "database_table_or_view.database_schema.database.name", "database_table_or_view.database_schema.database.host.name" ],
        "types": [ "table_analysis" ]
      };
  
      igcrest.search(staleTables, function (err, resSearch) {
  
        const aTablesToAnalyze = [];
  
        // Build up a list of any tables with analysis results, so we can compare against project tables  
        const tablesWithAnalysisResults = {};
        for (let i = 0; i < resSearch.items.length; i++) {
          const item = resSearch.items[i];
          //const sTblAnalysisName = item._name;
          //const sHostName = item["database_table_or_view.database_schema.database.host.name"];
          const sDbName = item["database_table_or_view.database_schema.database.name"];
          const sSchemaName = item["database_table_or_view.database_schema.name"];
          const sTblName = item["database_table_or_view.name"];
          const dModified = new Date(item.modified_on);
          const qualifiedName = sDbName + "." + sSchemaName + "." + sTblName;
          if (tablesWithAnalysisResults.hasOwnProperty(qualifiedName)) {
            // Find the most recent analysis result, in case there are multiple
            const lastTime = tablesWithAnalysisResults[qualifiedName];
            if (lastTime < dModified) {
              tablesWithAnalysisResults[qualifiedName] = dModified;
            }
          } else {
            tablesWithAnalysisResults[qualifiedName] = dModified;
          }
        }
  
        for (const key in projectTables) {
          if (projectTables.hasOwnProperty(key)) {
            // If there is no analysis result, it has never been analyzed -- add it
            if (!tablesWithAnalysisResults.hasOwnProperty(key)) {
              aTablesToAnalyze.push(key);
            } else {
              // Otherwise, check the date of the last analysis
              const lastAnalysis = tablesWithAnalysisResults[key];
              if (lastAnalysis <= timeToConsiderStale) {
                aTablesToAnalyze.push(key);
              }
            }
          }
        }
  
  // TODO: uncomment line below once files are working
        callback(err, aTablesToAnalyze);
  
      });
  
  // TODO: handle files
  // The tricky bit is that 'data_file' is just referenced as a 'main_object' -- so the last two properties below are not directly referenceable...
  // ... probably have to run a follow-up search on all 'data_file' objects where the 'data_file_records' contains a particular RID (?)
  //      "properties": [ "name", "modified_on", "data_file_record.name", "data_file_record.data_file.path", "data_file_record.data_file.host.name" ],
  
      const staleFiles = {
        "pageSize": "10000",
        "properties": [ "name", "modified_on", "data_file_record.name" ],
        "types": [ "file_record_analysis" ]
      };
  
      console.log("Searching files: " + JSON.stringify(staleFiles));
      igcrest.search(staleFiles, function (err, resSearch) {
    
        const aFilesToAnalyze = [];
  
        const filesWithAnalysisResults = {};
        for (let i = 0; i < resSearch.items.length; i++) {
          const item = resSearch.items[i];
          //const sFileAnalysisName = item._name;
          const sHostName = item["data_file_record.data_file.host.name"];
          const sFolderPath = item["data_file_record.data_file.path"];
          const sRecordName = item["data_file_record.name"];
          const dModified = new Date(item.modified_on);
          const qualifiedName = sHostName + ":" + sFolderPath + ":" + sRecordName;
          if (filesWithAnalysisResults.hasOwnProperty(qualifiedName)) {
            const lastTime = filesWithAnalysisResults[qualifiedName];
            if (lastTime < dModified) {
              filesWithAnalysisResults[qualifiedName] = dModified;
            }
          } else {
            console.log("Found analysis result for: " + qualifiedName);
            filesWithAnalysisResults[qualifiedName] = dModified;
          }
        }
  
        for (const key in projectFiles) {
          if (projectFiles.hasOwnProperty(key)) {
            // If there is no analysis result, it has never been analyzed -- add it
            if (!filesWithAnalysisResults.hasOwnProperty(key)) {
              console.log("Adding file to analyze: " + key);
              aFilesToAnalyze.push(key);
            } else {
              // Otherwise, check the date of the last analysis
              const lastAnalysis = filesWithAnalysisResults(key);
              if (lastAnalysis <= timeToConsiderStale) {
                console.log("Stale analysis: " + key);
                aFilesToAnalyze.push(key);
              } else {
                console.log("Ignoring -- not stale: " + key);
              }
            }
          }
        }
  
        callback(err, aFilesToAnalyze);
  
      });
  
  
    });
  
  };
  
  /**
   * Get the status of a running task
   *
   * @param {string} executionID - the unique identification number of the running task
   * @param {statusCallback} callback - callback that handles the response
   */
  const getTaskStatus = function(executionID, callback) {
    makeRequest('GET', "/ibm/iis/ia/api/analysisStatus?scheduleID=" + executionID, null, null, function(res, resStatus) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      const stat = {};
      const resDoc = new xmldom.DOMParser().parseFromString(resStatus);
      const nlExec = xpath.select("//*[local-name(.)='TaskExecution']", resDoc);
      if (nlExec.length > 1) {
        err = "More than one result found";
      }
      for (let i = 0; i < nlExec.length; i++) {
        stat.executionId = nlExec[i].getAttribute("executionId");
        stat.executionTime = nlExec[i].getAttribute("executionTime");
        stat.progress = nlExec[i].getAttribute("progress");
        stat.status = nlExec[i].getAttribute("status");
      }
      callback(err, stat);
      return stat;
    });
  };
  
  /**
   * Retrieves any execution IDs from the provided response
   *
   * @param {string} resXML
   * @returns {string[]} an array of execution IDs
   */
  const getExecutionIDsFromResponse = function(resXML) {
  
    const executionIDs = [];
    const resDoc = new xmldom.DOMParser().parseFromString(resXML);
    const nlTask = xpath.select("//*[local-name(.)='ScheduledTask']", resDoc);
    for (let i = 0; i < nlTask.length; i++) {
      const executionID = nlTask[i].getAttribute("scheduleId");
      executionIDs.push(executionID);
    }
    return executionIDs;
  
  };
  
  /**
   * Issues a request to reindex Solr for any resutls to appear appropriately in the IA Thin Client
   *
   * @param {int} batchSize - The batch size to retrieve information from the database. Increasing this size may improve performance but there is a possibility of reindex failure. The default is 25. The maximum value is 1000.
   * @param {int} solrBatchSize - The batch size to use for Solr indexing. Increasing this size may improve performance. The default is 100. The maximum value is 1000.
   * @param {boolean} upgrade - Specifies whether to upgrade the index schema from a previous version, and is a one time requirement when upgrading from one version of the thin client to another. The schema upgrade can be used to upgrade from any previous version of the thin client. The value true will upgrade the index schema. The value false is the default, and will not upgrade the index schema.
   * @param {boolean} force - Specifies whether to force reindexing if indexing is already in process. The value true will force a reindex even if indexing is in process. The value false is the default, and prevents a reindex if indexing is already in progress. This option should be used if a previous reindex request is aborted for any reason. For example, if InfoSphere Information Server services tier system went offline, you would use this option.
   * @param {reindexCallback} callback - status of the reindex ["REINDEX_SUCCESSFUL"]
   */
  const reindexThinClient = function(batchSize, solrBatchSize, upgrade, force, callback) {
    let request = "/ibm/iis/dq/da/rest/v1/reindex";
    request = request +
              "?batchSize=" + _getValueOrDefault(batchSize, 25) +
              "&solrBatchSize=" + _getValueOrDefault(solrBatchSize, 100) +
              "&upgrade=" + _getValueOrDefault(upgrade, false) +
              "&force=" + _getValueOrDefault(force, true);
    makeRequest('GET', request, null, null, function(res, resStatus) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      }
      callback(err, resStatus);
      return resStatus;
    });
  };
  
  /**
   * Retrieves a listing of any records that failed a particular Data Rule or Data Rule Set (its latest execution)
   *
   * @param {string} projectName - The name of the Information Analyzer project in which the Data Rule or Data Rule Set exists
   * @param {string} ruleOrSetName - The name of the Data Rule or Data Rule Set
   * @param {int} numRows - The maximum number of rows to retrieve (if unspecified will default to 100)
   * @param {recordsCallback} callback - the records that failed
   */
  const getRuleExecutionFailedRecordsFromLastRun = function(projectName, ruleOrSetName, numRows, callback) {
    let request = "/ibm/iis/ia/api/executableRule/outputTable";
    request = request +
              "?projectName=" + encodeURI(projectName) +
              "&ruleName=" + encodeURI(ruleOrSetName);
    if (numRows !== undefined && numRows !== null) {
      request = request + "&nbOfRows=" + numRows;
    } else {
      request = request + "&nbOfRows=100";
    }
    makeRequest('GET', request, null, null, function(res, resRecords) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      } else {
        const aRows = [];
        const colMap = {};
        const resDoc = new xmldom.DOMParser().parseFromString(resRecords);
        const nlCols = xpath.select("//*[local-name(.)='OutputColumn']", resDoc);
        const nlRows = xpath.select("//*[local-name(.)='Row']", resDoc);
        const aColNames = [];
        for (let i = 0; i < nlCols.length; i++) {
          const colName = nlCols[i].getAttribute("name");
          aColNames.push(colName);
          colMap[colName] = nlCols[i].getAttribute("value");
        }
        for (let i = 0; i < nlRows.length; i++) {
          const nlCells = nlRows[i].getElementsByTagName("Value");
          const rowVals = {};
          for (let j = 0; j < nlCells.length; j++) {
            const value = nlCells[j].textContent;
            const colName = aColNames[j];
            rowVals[colName] = value;
          }
          aRows.push(rowVals);
        }
        callback(err, aRows, colMap);
        return aRows;
      }
    });
  };
  
  /**
   * Retrieves the statistics of the executions of a particular Data Rule or Data Rule Set
   *
   * @param {string} projectName - The name of the Information Analyzer project in which the Data Rule or Data Rule Set exists
   * @param {string} ruleOrSetName - The name of the Data Rule or Data Rule Set
   * @param {boolean} bLatestOnly - If true, returns only the statistics from the latest execution (otherwise full history)
   * @param {statsCallback} callback - the statistics of the historical execution(s)
   */
  const getRuleExecutionResults = function(projectName, ruleOrSetName, bLatestOnly, callback) {
    let request = "/ibm/iis/ia/api/executableRule/executionHistory";
    request = request +
              "?projectName=" + encodeURI(projectName) +
              "&ruleName=" + encodeURI(ruleOrSetName);
    makeRequest('GET', request, null, null, function(res, resStats) {
      let err = null;
      if (res.statusCode !== 200) {
        err = "Unsuccessful request " + res.statusCode;
        console.error(err);
        console.error('headers: ', res.headers);
        throw new Error(err);
      } else {
        const aStats = [];
        const resDoc = new xmldom.DOMParser().parseFromString(resStats);
        const nlResults = xpath.select("//*[local-name(.)='RuleExecutionResult']", resDoc);
        for (let i = 0; i < nlResults.length; i++) {
          const sample = nlResults[i].getElementsByTagName("RuntimeMetaData")[0].getAttribute("sampleUsed");
          if (sample === "false") { // only take full runs, not samples
            const stat = {
              id: nlResults[i].getAttribute("id"),
              dStart: nlResults[i].getAttribute("startTime"),
              dEnd: nlResults[i].getAttribute("endTime"),
              numFailed: nlResults[i].getAttribute("nbFailed"),
              numTotal: nlResults[i].getAttribute("nbOfRecords"),
              status: nlResults[i].getAttribute("status")
            };
            aStats.push(stat);
            if (bLatestOnly) {
              i = nlResults.length;
            }
          }
        }
        callback(err, aStats);
        return aStats;
      }
    });
  };
  
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
   * This callback is invoked as the result of an IA REST API call to retrieve records that failed Data Rules
   * @callback recordsCallback
   * @param {string} errorMessage - any error message, or null if no errors
   * @param {Object[]} records - an array of records, each record being a JSON object keyed by column name and with the value of the column for that row
   * @param {Object} columnMap - key-value pairs mapping column names to their context (e.g. full identity in the case of database columns like RecordPK)
   */
  
  /**
   * This callback is invoked as the result of an IA REST API call to retrieve historical statistics on Data Rule executions
   * @callback statsCallback
   * @param {string} errorMessage - any error message, or null if no errors
   * @param {Object[]} stats - an array of stats, each stat being a JSON object with ???
   */
  
   /**
    * This callback is invoked as the result of retrieving a list of items that Information Analyzer should ignore
    * @callback itemsToIgnoreCallback
    * @param {string} errorMessage - any error message, or null if no errors
    * @param {Object} typeToIdentities - dictionary keyed by object type, with each value being an array of objects of that type to ignore (as identity strings, /-delimited)
    */

  return {
    setConnection: setConnection,
    makeRequest: makeRequest,
    getAllItemsToIgnore: getAllItemsToIgnore,
    addIADBToIgnoreList: addIADBToIgnoreList,
    createOrUpdateAnalysisProject: createOrUpdateAnalysisProject,
    getProjectList: getProjectList,
    getProjectDataSourceList: getProjectDataSourceList,
    runColumnAnalysis: runColumnAnalysis,
    runColumnAnalysisForSources: runColumnAnalysisForSources,
    publishResults: publishResults,
    publishResultsForSources: publishResultsForSources,
    getStaleAnalysisResults: getStaleAnalysisResults,
    getTaskStatus: getTaskStatus,
    getExecutionIDsFromResponse: getExecutionIDsFromResponse,
    reindexThinClient: reindexThinClient,
    getRuleExecutionFailedRecordsFromLastRun: getRuleExecutionFailedRecordsFromLastRun,
    getRuleExecutionResults: getRuleExecutionResults
  };

})();

module.exports = RestIA;

if (typeof require === 'function') {
  exports.Project = Project;
  exports.ColumnAnalysis = ColumnAnalysis;
  exports.PublishResults = PublishResults;
}
