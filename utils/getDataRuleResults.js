#!/usr/bin/env node

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
 * @file Gets the output table from the execution of a Data Rule or Data Rule Set
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires ibm-iis-commons
 * @requires yargs
 * @example
 * @example
 * // retrieves the results of the last run of the data rule named 'IndustryCodeMustExist'
 * ./getDataRuleResults.js -n 'IndustryCodeMustExist' -d hostname:9445 -u isadmin -p isadmin
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const Table = require('cli-table');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -i <project> -n <name> -d <host>:<port> -u <user> -p <password>')
    .option('i', {
      alias: 'project',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('n', {
      alias: 'name',
      describe: 'Name of the Data Rule or Data Rule Set',
      demand: true, requiresArg: true, type: 'string'
    })
    .env('DS')
    .option('d', {
      alias: 'domain',
      describe: 'Host and port for invoking IA REST',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('u', {
      alias: 'deployment-user',
      describe: 'User for invoking IA REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .option('p', {
      alias: 'deployment-user-password',
      describe: 'Password for invoking IA REST',
      demand: true, requiresArg: true, type: 'string',
      default: "isadmin"
    })
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;

// Base settings
const host_port = argv.domain.split(":");

const restConnect = new commons.RestConnection(argv.deploymentUser, argv.deploymentUserPassword, host_port[0], host_port[1]);
iarest.setConnection(restConnect);

const projectName = argv.project;
const receivedRuleName = argv.name;

iarest.getRuleExecutionFailedRecordsFromLastRun(projectName, receivedRuleName, null, function(err, aResults) {

  if (aResults.length > 0) {
    const aColNames = Object.keys(aResults[0]);
    const table = new Table({
      head: aColNames
    });
    for (let i = 0; i < aResults.length; i++) {
      const rowData = [];
      for (let j = 0; j < aColNames.length; j++) {
        const value = aResults[i][aColNames[j]];
        rowData.push(value);
      }
      table.push(rowData);
    }
    outputResultRecords(table);
  }

});

function outputResultRecords(table) {
  console.log(table.toString());
}
