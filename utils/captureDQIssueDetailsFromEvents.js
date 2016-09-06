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

/**
 * @file Gets the output table from the execution of a Data Rule or Data Rule Set
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires kafka-node
 * @requires yargs
 * @example
 * @example
 * // retrieves the results of the last run of the data rule named 'IndustryCodeMustExist'
 * ./captureDQIssueDetailsFromEvents.js -d hostname:9445 -z hostname:52181
 */

var iarest = require('ibm-ia-rest');
var kafka = require('kafka-node');
var Table = require('cli-table');
require('shelljs/global');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -d <host>:<port> -z <host>:<port> -u <username> -p <password>')
    .env('DS')
    .option('d', {
      alias: 'domain',
      describe: 'Host and port for invoking IA REST',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('z', {
      alias: 'zookeeper',
      describe: 'Host and port for Zookeeper connection to consume from Kafka',
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
var bContinueOnError = true;
var host_port = argv.domain.split(":");

iarest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
iarest.setServer(host_port[0], host_port[1]);

var HighLevelConsumer = kafka.HighLevelConsumer;
var client = new kafka.Client(argv.zookeeper, 'captureDQIssueDetailsFromEvents');
var consumer = new HighLevelConsumer(client, [{ topic: 'InfosphereEvents' }]);

consumer.on('message', function(message) {

  //console.log(message);

  var jsonMsg = JSON.parse(message.value);
  if (jsonMsg.eventType === "NEW_EXCEPTIONS_EVENT") {

    if (jsonMsg.applicationType === "Exception Stage") {

      var tableName = jsonMsg.exceptionSummaryUID;
      console.log("Table: " + tableName);
      // TODO: retrieve details from the Exceptions database:
      //       - failed records
      //       - name of the job
      //       - stage within the job

    } else if (jsonMsg.applicationType === "Information Analyzer") {

      var cliResult = exec("./getMetadataForExecutableDataRules.js -n " + jsonMsg.exceptionSummaryName + " -d " + argv.domain + " -u " + argv.deploymentUser + " -p " + argv.deploymentUserPassword, {silent: false, "shell": "/bin/bash"});

      iarest.getRuleExecutionResults(jsonMsg.projectName, jsonMsg.exceptionSummaryName, true, function(err, aStats) {
        console.log(JSON.stringify(aStats[0]));
      });

      iarest.getRuleExecutionFailedRecordsFromLastRun(jsonMsg.projectName, jsonMsg.exceptionSummaryName, null, function(err, aResults) {
    
        if (aResults.length > 0) {
          var aColNames = Object.keys(aResults[0]);
          var table = new Table({
            head: aColNames
          });
          for (var i = 0; i < aResults.length; i++) {
            var rowData = [];
            for (var j = 0; j < aColNames.length; j++) {
              var value = aResults[i][aColNames[j]];
              rowData.push(value);
            }
            table.push(rowData);
          }
          outputResultRecords(table);
        }
    
      });

    }

  }

});

function outputResultRecords(table) {
  console.log(table.toString());
}
