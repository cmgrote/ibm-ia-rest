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
 * @file Refresh the column analysis for any assets in the specified import area
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires ibm-iis-commons
 * @requires progress
 * @requires yargs
 * @example
 * // refreshes column analysis for all columns and file fields with results older than 48 hours, within the "Automated Profiling" project (by default)
 * ./refreshColumnAnalysis.js -t 48 -d hostname:9445 -u isadmin -p isadmin
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const ProgressBar = require('progress');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -n <name> -t <timeInHours> -d <host>:<port> -u <user> -p <password>')
    .option('n', {
      alias: 'name',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string',
      default: "Automated Profiling"
    })
    .option('t', {
      alias: 'time',
      describe: 'Re-run analysis on any assets without results published in the last T hours',
      requiresArg: true, type: 'number'
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

const projectName = argv.name;
const lastRefreshTime = argv.time;
const now = new Date();
let staleBefore = new Date();
if (lastRefreshTime !== undefined && lastRefreshTime !== "") {
  staleBefore = staleBefore.setHours(now.getHours() - lastRefreshTime);
}

const bar = new ProgressBar('  analyzing [:bar] :percent  (:execId)', {
  complete: '=',
  incomplete: ' ',
  width: 20,
  total: 100
});

console.log("Determining stale analyses...");
iarest.getStaleAnalysisResults(projectName, staleBefore, function(err, aStaleSources) {

  console.log("  running column analysis for " + aStaleSources.length + " sources.");
  iarest.runColumnAnalysisForSources(projectName, aStaleSources, function(err, results) {

    const aIDs = iarest.getExecutionIDsFromResponse(results);
    // TODO: confirm if there will always only be a single execution ID (?)
    if (aIDs.length > 0) {
      const execId = aIDs[0];
      const timer = setInterval(waitForCompletion, 10000, execId, function (status) {
  
        clearInterval(timer);
        if (status === "successful") {
      
          console.log("Publishing results...");
          iarest.publishResultsForSources(projectName, aStaleSources, function(err, statusPublish) {
        
            console.log("  status: " + statusPublish);
            console.log("Reindexing Solr for the thin client...");
            iarest.reindexThinClient(25, 100, false, true, function(err, resIndex) {
              console.log("  status: " + resIndex);
            });
      
          });
      
        } else {
          process.exit(1);
        }
      
      });

    }

  });

});

function waitForCompletion(id, callback) {  
  iarest.getTaskStatus(id, function(err, results) {
    const status = results.status;
    const progress = results.progress;
    if (status === "running") {
      bar.update(progress/100, {'execId': results.executionId});
    } else if (status === "successful") {
      console.log("\n  completed successfully after " + results.executionTime + " ms");
      callback(status, results);
    } else {
      console.warn("\n  problem completing: " + JSON.stringify(results));
      callback(status, results);
    }
  });
}
