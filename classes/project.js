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

const xmldom = require('xmldom');

/**
 * Project class -- for handling Information Analyzer projects
 * @license Apache-2.0
 */
class Project {

  constructor(name) {
    this._doc = new xmldom.DOMImplementation().createDocument("http://www.ibm.com/investigate/api/iaapi", "iaapi:Project", null);
    this._doc.documentElement.setAttribute("xmlns:iaapi", "http://www.ibm.com/investigate/api/iaapi");
    this._doc.documentElement.setAttribute("name", name);
    this._doc.normalize();
  }

  /**
   * Retrieve the Project document
   * 
   * @function
   */
  getProjectDoc() {
    return this._doc;
  }

  /**
   * Set the description of the project
   *
   * @function
   */
  setDescription(desc) {
    const eDesc = this._doc.createElement("description");
    const txt = this._doc.createTextNode(desc);
    eDesc.appendChild(txt);
    this._doc.documentElement.appendChild(eDesc);
  }

  /**
   * Add the specified table to the project
   *
   * @function
   * @param {string} datasource - the database name 
   * @param {string} schema
   * @param {string} table
   * @param {string[]} aColumns - array of column names
   */
  addTable(datasource, schema, table, aColumns) {
    
    let nDS = this._doc.getElementsByTagName("DataSources");
    if (nDS.length === 0) {
      nDS = this._doc.createElement("DataSources");
      this._doc.documentElement.appendChild(nDS);
    } else {
      nDS = nDS[0];
    }
    
    const eDS = this._doc.createElement("DataSource");
    eDS.setAttribute("name", datasource);
    nDS.appendChild(eDS);
    const eS = this._doc.createElement("Schema");
    eS.setAttribute("name", schema);
    eDS.appendChild(eS);
    const eT = this._doc.createElement("Table");
    eT.setAttribute("name", table);
    eS.appendChild(eT);

    for (let i = 0; i < aColumns.length; i++) {
      const sColName = aColumns[i];
      const eC = this._doc.createElement("Column");
      eC.setAttribute("name", sColName);
      eT.appendChild(eC);
    }

  }

  /**
   * Add the specified file to the project
   *
   * @function
   * @param {string} datasource - the host name?
   * @param {string} folder - the full path to the file
   * @param {string} file - the name of the file
   * @param {string[]} aFields - array of field names within the file
   */
  addFile(datasource, folder, file, aFields) {

    let nDS = this._doc.getElementsByTagName("DataSources");
    if (nDS.length === 0) {
      nDS = this._doc.createElement("DataSources");
      this._doc.documentElement.appendChild(nDS);
    } else {
      nDS = nDS[0];
    }

    const eDS = this._doc.createElement("DataSource");
    eDS.setAttribute("name", datasource);
    nDS.appendChild(eDS);
    const eFolder = this._doc.createElement("FileFolder");
    eFolder.setAttribute("name", folder);
    eDS.appendChild(eFolder);
    const eFile = this._doc.createElement("FileName");
    eFile.setAttribute("name", file);
    eFolder.appendChild(eFile);

    for (let i = 0; i < aFields.length; i++) {
      const sFieldName = aFields[i];
      const eField = this._doc.createElement("Column");
      eField.setAttribute("name", sFieldName);
      eFile.appendChild(eField);
    }

  }

}

module.exports = Project;
