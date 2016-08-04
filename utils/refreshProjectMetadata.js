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
 * @file Refresh the metadata handled by the specified import areas
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires yargs
 * @example
 * // refreshes all of the metadata that can be found in IGC into the Information Analyzer project (by default called "Automated Profiling")
 * ./refreshProjectMetadata.js -d hostname:9445 -u isadmin -p isadmin
 * @example
 * // refreshes all of the metadata that was updated in IGC in the last 24 hours into the Information Analyzer project (by default called "Automated Profiling")
 * ./refreshProjectMetadata.js -t 24 -d hostname:9445 -u isadmin -p isadmin
 */

var iarest = require('ibm-ia-rest');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -n <name> -x <description> -t <numberOfHours> -d <host>:<port> -u <user> -p <password>')
    .option('n', {
      alias: 'name',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string',
      default: "Automated Profiling"
    })
    .option('x', {
      alias: 'desc',
      describe: 'Description of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string',
      default: "A base project for the automation of profiling"
    })
    .option('t', {
      alias: 'time',
      describe: 'Update the project with anything in IGC modified within the last T hours',
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
var bContinueOnError = true;
var host_port = argv.domain.split(":");
var lastRefreshTime = argv.time;

iarest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
iarest.setServer(host_port[0], host_port[1]);

var prjName = argv.name;
var prjDesc = argv.desc;

var now = new Date();
var addAfter = new Date();
if (lastRefreshTime !== undefined && lastRefreshTime !== "") {
  addAfter = addAfter.setHours(now.getHours() - lastRefreshTime);
}

iarest.addIADBToIgnoreList(function(err, resIgnore) {
  iarest.getProjectList(function(err, resList) {
    // TODO: replace database hard-coding (shouldn't be based on a pre-defined type at all, just timestamp?)
    iarest.createOrUpdateAnalysisProject(prjName, prjDesc, "database", addAfter, function(err, results) {
      console.log(results);
    });
  });
});
