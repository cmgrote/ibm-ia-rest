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

function _getValueOrDefault(val, def) {
  return (val === undefined) ? def : val;
}

/**
 * ColumnAnalysis class -- for handling Information Analyzer column analysis tasks
 */
class ColumnAnalysis {

  /**
   * @constructor
   * @param {Project} project - the project in which to create the column analysis task
   * @param {boolean} analyzeColumnProperties - whether or not to analyze column properties
   * @param {string} captureResultsType - specifies the type of frequency distribution results that are written to the analysis database ["CAPTURE_NONE", "CAPTURE_ALL", "CAPTURE_N"]
   * @param {int} minCaptureSize - the minimum number of results that are written to the analysis database, including both typical and atypical values
   * @param {int} maxCaptureSize - the maximum number of results that are written to the analysis database
   * @param {boolean} analyzeDataClasses - whether or not to analyze data classes
   */
  constructor(project, analyzeColumnProperties, captureResultsType, minCaptureSize, maxCaptureSize, analyzeDataClasses) {

    this._doc = project.getProjectDoc();
      
    analyzeColumnProperties = _getValueOrDefault(analyzeColumnProperties, true);
    captureResultsType = _getValueOrDefault(captureResultsType, "CAPTURE_ALL");
    minCaptureSize = _getValueOrDefault(minCaptureSize, 5000);
    maxCaptureSize = _getValueOrDefault(maxCaptureSize, 10000);
    analyzeDataClasses = _getValueOrDefault(analyzeDataClasses, false);
  
    const eRCA = this._doc.createElement("RunColumnAnalysis");
    eRCA.setAttribute("analyzeColumnProperties", analyzeColumnProperties);
    eRCA.setAttribute("captureFDResultsType", captureResultsType);
    eRCA.setAttribute("minFDCaptureSize", minCaptureSize);
    eRCA.setAttribute("maxFDCaptureSize", maxCaptureSize);
    eRCA.setAttribute("analyzeDataClasses", analyzeDataClasses);
  
    let task = this._doc.getElementsByTagName("Tasks");
    if (task.length === 0) {
      task = this._doc.createElement("Tasks");
      this._doc.documentElement.appendChild(task);
    } else {
      task = task[0];
    }
  
    task.appendChild(eRCA);
  
  }

  /**
   * Use to (optionally) set any sampling options for the column analysis
   *
   * @function
   * @param {string} type - the sampling type ["random", "sequential", "every_nth"]
   * @param {number} size - if less than 1.0, the percentage of values to use in the sample; otherwise the maximum number of records in the sample.  If you use the "random" type of data sample, specify the sample size that is the same number as the number of records that will be in the result, based on the value that you specify in the Percent field. Otherwise, the results might be skewed.
   * @param {string} seed - if type is "random", this value is used to initialize the random generators (two samplings that use the same seed value will contain the same records)
   * @param {int} step - if type is "every_nth", this value indicates the step to apply (one row will be kept out of every nth value rows)
   */
  setSampleOptions(type, size, seed, step) {
    const eSO = this._doc.createElement("SampleOptions");
    eSO.setAttribute("type", type);
    if (size <= 1.0) {
      eSO.setAttribute("percent", size);
    } else {
      eSO.setAttribute("size", size);
    }
    if (type === "random") {
      eSO.setAttribute("seed", seed);
    } else if (type === "every_nth") {
      eSO.setAttribute("nthValue", step);
    }
    this._doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eSO);
  }

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
  setEngineOptions(retainOSH, retainData, config, gridEnabled, requestedNodes, minNodes, partitionsPerNode) {
    const eEO = this._doc.createElement("EngineOptions");
    eEO.setAttribute("retainOsh", retainOSH);
    eEO.setAttribute("retainDataSets", retainData);
    eEO.setAttribute("PXConfigurationFile", config);
    eEO.setAttribute("gridEnabled", gridEnabled);
    eEO.setAttribute("requestedNodes", requestedNodes);
    eEO.setAttribute("minimumNodes", minNodes);
    eEO.setAttribute("partitionsPerNode", partitionsPerNode);
    this._doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eEO);
  }

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
  setJobOptions(debugEnabled, numDebuggedRecords, arraySize, autoCommit, isolationLevel, updateExistingTables) {
    const eJO = this._doc.createElement("JobOptions");
    eJO.setAttribute("debugEnabled", debugEnabled);
    eJO.setAttribute("nbOfDebuggedRecords", numDebuggedRecords);
    eJO.setAttribute("arraySize", arraySize);
    eJO.setAttribute("autoCommit", autoCommit);
    eJO.setAttribute("isolationLevel", isolationLevel);
    eJO.setAttribute("updateExistingTables", updateExistingTables);
    this._doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eJO);
  }

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
  addColumn(datasource, schema, table, column, hostname) {
    let name = datasource + "." + schema + "." + table + "." + column;
    // TODO: determine correct way of specifying fully-qualified name that includes hostname (as it has dots in it itself, will cause a 500 response...)
    if (hostname !== undefined) {
      //name = hostname + "." + name;
    }
    const eC = this._doc.createElement("Column");
    eC.setAttribute("name", name);
    this._doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eC);
  }

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
  addFileField(connection, path, filename, column, hostname) {
    let name = connection + ":" + path + ":" + filename + ":" + column;
    if (hostname !== undefined) {
      name = hostname + ":" + name;
    }
    const eF = this._doc.createElement("Column");
    eF.setAttribute("name", name);
    this._doc.getElementsByTagName("RunColumnAnalysis").item(0).appendChild(eF);
  }

}

module.exports = ColumnAnalysis;
