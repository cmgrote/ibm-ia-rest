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
 * @file Keep the metadata up-to-date in the base project from which all automated profiling can be driven
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires yargs
 * @example
 * // updates the base automation project, using a default name of "Automated Profiling"
 * ./updateProjectMetadata.js -d hostname:9445 -u isadmin -p isadmin
 */

//const fs = require('fs-extra');
//const pd = require('pretty-data').pd;
//const xmldom = require('xmldom');
var iarest = require('ibm-ia-rest');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -n <name> -x <description> -d <host>:<port> -u <user> -p <password>')
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
    .env('DS')
    .option('d', {
      alias: 'domain',
      describe: 'Host and port for invoking IA REST',
      demand: true, requiresArg: true, type: 'string',
      default: "services:9445"
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
var host_port = argv.domain.split(":");
iarest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
iarest.setServer(host_port[0], host_port[1]);

iarest.createAnalysisProject(argv.name, argv.desc, "database", function(err, results) {
  //console.error(err);
  console.log(results);
});
