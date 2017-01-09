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
 * @file PublishResults class -- for handling Information Analyzer results publishing tasks
 * @license Apache-2.0
 */

/**
 * @namespace
 */
class PublishResults {

  /**
   * @constructor
   * @param {Project} project - the project from which to publish analysis results
   */
  constructor(project) {
  
    this._doc = project.getProjectDoc();
      
    const ePR = this._doc.createElement("PublishResults");
  
    let task = this._doc.getElementsByTagName("Tasks");
    if (task.length === 0) {
      task = this._doc.createElement("Tasks");
      this._doc.documentElement.appendChild(task);
    } else {
      task = task[0];
    }
  
    task.appendChild(ePR);
  
  }

  /**
   * Use to add a table whose results should be published -- the table can be '*' to specify all tables
   *
   * @function
   * @param {string} datasource
   * @param {string} schema
   * @param {string} table
   * @param {string} [hostname]
   */
  addTable(datasource, schema, table, hostname) {
    let name = datasource + "." + schema + "." + table;
    // TODO: determine correct way of specifying fully-qualified name that includes hostname (as has dots in it itself, will cause a 500 response)
    if (hostname !== undefined) {
      //name = hostname.toUpperCase() + "." + name;
    }
    const eC = this._doc.createElement("Table");
    eC.setAttribute("name", name);
    this._doc.getElementsByTagName("PublishResults").item(0).appendChild(eC);
  }

  /**
   * Use to add a file whose results should be published -- file can be '*' to specify all files
   *
   * @function
   * @param {string} connection - e.g. "HDFS"
   * @param {string} path - directory path, not including the filename
   * @param {string} filename
   * @param {string} [hostname]
   */
  addFile(connection, path, filename, hostname) {
    let name = connection + ":" + path + ":" + filename;
    if (hostname !== undefined) {
      name = hostname.toUpperCase() + ":" + name;
    }
    const eF = this._doc.createElement("Table");
    eF.setAttribute("name", name);
    this._doc.getElementsByTagName("PublishResults").item(0).appendChild(eF);
  }

}

module.exports = PublishResults;
