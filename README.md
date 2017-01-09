# README

<!-- Generated by documentation.js. Update this documentation by updating the source code. -->

## RestIA

Re-usable functions for interacting with IA's REST API

**Examples**

```javascript
// runs column analysis for any objects in Automated Profiling that have not been analyzed since the moment the script is run (new Date())
var iarest = require('ibm-ia-rest');
var commons = require('ibm-iis-commons');
var restConnect = new commons.RestConnection("isadmin", "isadmin", "hostname", "9445");
iarest.setConnection(restConnect);
iarest.getStaleAnalysisResults("Automated Profiling", new Date(), function(errStale, aStaleSources) {
  iarest.runColumnAnalysisForDataSources(aStaleSources, function(errExec, tamsAnalyzed) {
    // Note that the API returns async; if you want to busy-wait you need to poll events on Kafka
  });
});
```

**Meta**

-   **license**: Apache-2.0

## ibm-ia-rest

## setConnection

Set the connection for the REST API

**Parameters**

-   `restConnect` **RestConnection** RestConnection object, from ibm-iis-commons

## makeRequest

Make a request against IA's REST API

**Parameters**

-   `method` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** type of request, one of ['GET', 'PUT', 'POST', 'DELETE']
-   `path` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the path to the end-point (e.g. /ibm/iis/ia/api/...)
-   `input` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** any input for the request, i.e. for PUT, POST
-   `inputType` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** the type of input, if any provided ['text/xml', 'application/json']
-   `callback` **[requestCallback](#requestcallback)** callback that handles the response


-   Throws **any** will throw an error if connectivity details are incomplete or there is a fatal error during the request

## getAllItemsToIgnore

Retrieves a list of all items that should be ignored, i.e. where they are labelled with "Information Analyzer Ignore List"

**Parameters**

-   `callback` **[itemsToIgnoreCallback](#itemstoignorecallback)** 

## addIADBToIgnoreList

Adds the IADB schema to a list of objects for Information Analyzer to ignore (to prevent them being added to projects or being analysed); this is accomplished by creating a label 'Information Analyzer Ignore List'

**Parameters**

-   `callback` **[requestCallback](#requestcallback)** callback that handles the response

## createOrUpdateAnalysisProject

Create or update an analysis project, to include ALL objects known to IGC that were updated after the date received -- necessary before any tasks can be executed

**Parameters**

-   `name` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** name of the project
-   `description` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** description of the project
-   `updatedAfter` **[Date](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date)?** include into the project any objects in IGC last updated after this date
-   `callback` **[requestCallback](#requestcallback)** callback that handles the response

## getProjectList

Get a list of Information Analyzer projects

**Parameters**

-   `callback` **[listCallback](#listcallback)** callback that handles the response

## getProjectDataSourceList

Get a list of all of the data sources in the specified Information Analyzer project

**Parameters**

-   `projectName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `callback` **[listCallback](#listcallback)** callback that handles the response (will be entries with HOST||DB.SCHEMA.TABLE and HOST||PATH:FILE)

## runColumnAnalysisForDataSources

Run a full column analysis against the list of data sources specificed (based on TAM RIDs)

**Parameters**

-   `aDataSources` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)>** an array of data sources, as returned by getProjectDataSourceList
-   `callback` **[requestCallback](#requestcallback)** callback that handles the response

## publishResultsForDataSources

Publish analysis results for the list of data sources specified

**Parameters**

-   `projectRID` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** RID of the IA project
-   `aTAMs` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)>** an array of TAM RIDs whose analysis should be published
-   `callback` **[requestCallback](#requestcallback)** callback that handles the response

## getStaleAnalysisResults

Retrieve previously published analysis results

**Parameters**

-   `projectName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** name of the IA project
-   `timeToConsiderStale` **[Date](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date)** the time before which any analysis results should be considered stale
-   `callback` **[requestCallback](#requestcallback)** callback that handles the response

## reindexThinClient

Issues a request to reindex Solr for any resutls to appear appropriately in the IA Thin Client

**Parameters**

-   `batchSize` **int** The batch size to retrieve information from the database. Increasing this size may improve performance but there is a possibility of reindex failure. The default is 25. The maximum value is 1000.
-   `solrBatchSize` **int** The batch size to use for Solr indexing. Increasing this size may improve performance. The default is 100. The maximum value is 1000.
-   `upgrade` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** Specifies whether to upgrade the index schema from a previous version, and is a one time requirement when upgrading from one version of the thin client to another. The schema upgrade can be used to upgrade from any previous version of the thin client. The value true will upgrade the index schema. The value false is the default, and will not upgrade the index schema.
-   `force` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** Specifies whether to force reindexing if indexing is already in process. The value true will force a reindex even if indexing is in process. The value false is the default, and prevents a reindex if indexing is already in progress. This option should be used if a previous reindex request is aborted for any reason. For example, if InfoSphere Information Server services tier system went offline, you would use this option.
-   `callback` **[reindexCallback](#reindexcallback)** status of the reindex ["REINDEX_SUCCESSFUL"]

## getRuleExecutionFailedRecordsFromLastRun

Retrieves a listing of any records that failed a particular Data Rule or Data Rule Set (its latest execution)

**Parameters**

-   `projectName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** The name of the Information Analyzer project in which the Data Rule or Data Rule Set exists
-   `ruleOrSetName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** The name of the Data Rule or Data Rule Set
-   `numRows` **int** The maximum number of rows to retrieve (if unspecified will default to 100)
-   `callback` **[recordsCallback](#recordscallback)** the records that failed

## getRuleExecutionResults

Retrieves the statistics of the executions of a particular Data Rule or Data Rule Set

**Parameters**

-   `projectName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** The name of the Information Analyzer project in which the Data Rule or Data Rule Set exists
-   `ruleOrSetName` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** The name of the Data Rule or Data Rule Set
-   `bLatestOnly` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** If true, returns only the statistics from the latest execution (otherwise full history)
-   `callback` **[statsCallback](#statscallback)** the statistics of the historical execution(s)

## statsCallback

This callback is invoked as the result of an IA REST API call to retrieve historical statistics on Data Rule executions

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `stats` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)>** an array of stats, each stat being a JSON object with ???

## recordsCallback

This callback is invoked as the result of an IA REST API call to retrieve records that failed Data Rules

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `records` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)>** an array of records, each record being a JSON object keyed by column name and with the value of the column for that row
-   `columnMap` **[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)** key-value pairs mapping column names to their context (e.g. full identity in the case of database columns like RecordPK)

## reindexCallback

This callback is invoked as the result of an IA REST API call to re-index Solr for IATC

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `status` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the status of the reindex operation ["REINDEX_SUCCESSFUL"]

## statusCallback

This callback is invoked as the result of an IA REST API call, providing the response of that request.

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `status` **[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)** the response of the request, in the form of an object keyed by execution ID, with subkeys for executionTime, progress and status ["running", "successful", "failed", "cancelled"]

## listCallback

This callback is invoked as the result of an IA REST API call, providing the response of that request.

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `aResponse` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)>** the response of the request, in the form of an array

## requestCallback

This callback is invoked as the result of an IA REST API call, providing the response of that request.

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `responseXML` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the XML of the response

## itemsToIgnoreCallback

This callback is invoked as the result of retrieving a list of items that Information Analyzer should ignore

Type: [Function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/function)

**Parameters**

-   `errorMessage` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** any error message, or null if no errors
-   `typeToIdentities` **[Object](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)** dictionary keyed by object type, with each value being an array of objects of that type to ignore (as identity strings, /-delimited)

## Project

Project class -- for handling Information Analyzer projects

**Meta**

-   **license**: Apache-2.0

### getProjectDoc

Retrieve the Project document

### setDescription

Set the description of the project

**Parameters**

-   `desc`  

### addTable

Add the specified table to the project

**Parameters**

-   `datasource` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the database name
-   `schema` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `table` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `aColumns` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)>** array of column names

### addFile

Add the specified file to the project

**Parameters**

-   `datasource` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the host name?
-   `folder` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the full path to the file
-   `file` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the name of the file
-   `aFields` **[Array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)>** array of field names within the file

## ColumnAnalysis

ColumnAnalysis class -- for handling Information Analyzer column analysis tasks

**Meta**

-   **license**: Apache-2.0

### constructor

**Parameters**

-   `project` **[Project](#project)** the project in which to create the column analysis task
-   `analyzeColumnProperties` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether or not to analyze column properties
-   `captureResultsType` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** specifies the type of frequency distribution results that are written to the analysis database ["CAPTURE_NONE", "CAPTURE_ALL", "CAPTURE_N"]
-   `minCaptureSize` **int** the minimum number of results that are written to the analysis database, including both typical and atypical values
-   `maxCaptureSize` **int** the maximum number of results that are written to the analysis database
-   `analyzeDataClasses` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether or not to analyze data classes

### setSampleOptions

Use to (optionally) set any sampling options for the column analysis

**Parameters**

-   `type` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the sampling type ["random", "sequential", "every_nth"]
-   `size` **[number](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number)** if less than 1.0, the percentage of values to use in the sample; otherwise the maximum number of records in the sample.  If you use the "random" type of data sample, specify the sample size that is the same number as the number of records that will be in the result, based on the value that you specify in the Percent field. Otherwise, the results might be skewed.
-   `seed` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** if type is "random", this value is used to initialize the random generators (two samplings that use the same seed value will contain the same records)
-   `step` **int** if type is "every_nth", this value indicates the step to apply (one row will be kept out of every nth value rows)

### setEngineOptions

Use to (optionally) set any engine options to use when running the column analysis

**Parameters**

-   `retainOSH` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether to retain the generated DataStage job or not
-   `retainData` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether to retain generated data sets (ignored when data rules are running)
-   `config` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** specifies an alternative configuration file to use with the DataStage engine during this run
-   `gridEnabled` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** whether or not the grid view will be enabled
-   `requestedNodes` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the name of requested nodes
-   `minNodes` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the minimum number of nodes you want in the analysis
-   `partitionsPerNode` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** the number of partitions for each node in the analysis

### setJobOptions

Use to (optionally) set any job options to use when running the column analysis

**Parameters**

-   `debugEnabled` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether to generate a debug table containing the evaluation results of all functions and tests contained in the expression (only used for running data rules)
-   `numDebuggedRecords` **int** how many rows should be debugged, if debugEnabled is "true"
-   `arraySize` **int** the size of the array (?)
-   `autoCommit` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** 
-   `isolationLevel` **int** 
-   `updateExistingTables` **[boolean](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether to update existing tables in IADB or create new ones (only used for column analysis)

### addColumn

Use to add a column to the column analysis task -- both table and column can be '\*' to specify all tables or all columns

**Parameters**

-   `datasource` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `schema` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `table` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `column` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `hostname` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** 

### addFileField

Use to add a file field to the column analysis task -- column can be '\*' to specify all fields within the file

**Parameters**

-   `connection` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** e.g. "HDFS"
-   `path` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** directory path, not including the filename
-   `filename` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `column` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** name of the field within the file
-   `hostname` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** 

## PublishResults

PublishResults class -- for handling Information Analyzer results publishing tasks

**Meta**

-   **license**: Apache-2.0

### constructor

**Parameters**

-   `project` **[Project](#project)** the project from which to publish analysis results

### addTable

Use to add a table whose results should be published -- the table can be '\*' to specify all tables

**Parameters**

-   `datasource` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `schema` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `table` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `hostname` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** 

### addFile

Use to add a file whose results should be published -- file can be '\*' to specify all files

**Parameters**

-   `connection` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** e.g. "HDFS"
-   `path` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** directory path, not including the filename
-   `filename` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)** 
-   `hostname` **[string](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String)?** 
