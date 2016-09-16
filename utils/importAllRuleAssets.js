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
 * @file Loads all rule-related assets into an environment from the specified file
 * @license Apache-2.0
 * @requires shelljs
 * @requires yargs
 * @example
 * // imports all rule-related assets from the file /tmp/extract.tgz into the environment and DataStage project FWK, applying any mapings in /tmp/mapping.xml
 * ./importAllRuleAssets.js -d 'FWK' -e ENGINE.HOST.NAME -f /tmp/extract.tgz -u isadmin -p isadmin -m /tmp/mapping.xml
 */

require('shelljs/global');
const path = require('path');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -d <dsProjectName> -e <engineTierFQDN> -f <path> -u <user> -p <password> -m <mappingFile>')
    .option('d', {
      alias: 'dsname',
      describe: 'Name of the DataStage project',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('e', {
      alias: 'engine',
      describe: 'Fully-qualified hostname of engine tier',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('f', {
      alias: 'filepath',
      describe: 'Path to the file in which to save assets',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('m', {
      alias: 'mapping',
      describe: 'Path to the file in which mappings are defined',
      demand: false, requiresArg: true, type: 'string'
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

const parsedPath = path.parse(argv.filepath);
const tmpPath = parsedPath.root + parsedPath.dir + path.sep + parsedPath.name;
const baseFilename = parsedPath.name;

let bCreated = false;
if (!test('-d', tmpPath)) {
  mkdir(tmpPath);
  bCreated = true;
}

const cmdTGZ = "tar zxv -C '" + tmpPath + "' -f '" + argv.filepath + "'";
const resultTGZ = exec(cmdTGZ, {silent: true, "shell": "/bin/bash"});
if (resultTGZ.code !== 0) {
  console.error("ERROR extracting single bundle TGZ:");
  console.error(resultTGZ.stderr);
  process.exit(resultTGZ.code);
}

console.log("Importing all Information Analyzer assets from '" + tmpPath + path.sep + baseFilename + ".isx'...");
let cmdImportIA = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh import" +
    " -u " + argv.deploymentUser +
    " -p " + argv.deploymentUserPassword +
    " -ar '" + tmpPath + path.sep + baseFilename + ".isx'" +
    " -replace -cm '' -ia '-onNameConflict replace' -report";
if (argv.mapping) {
  cmdImportIA = cmdImportIA + " -mapping '" + argv.mapping + "'";
}
const resultImportIA = exec(cmdImportIA, {silent: true, "shell": "/bin/bash"});
if (resultImportIA.code !== 0) {
  console.error("ERROR importing IA content:");
  console.error(resultImportIA.stderr);
  process.exit(resultImportIA.code);
}

// TODO: limit only to DataStage jobs that include Data Rules Stages (?)
console.log("Importing all DataStage assets from '" + tmpPath + path.sep + baseFilename + ".isx'...");
const cmdImportDS = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh import" +
    " -u " + argv.deploymentUser +
    " -p " + argv.deploymentUserPassword +
    " -ar '" + tmpPath + path.sep + baseFilename + ".isx'" +
    " -replace -ds '" + argv.engine + "/" + argv.dsname + "'";
const resultImportDS = exec(cmdImportDS, {silent: true, "shell": "/bin/bash"});
if (resultImportDS.code !== 0) {
  console.error("ERROR importing DS content:");
  console.error(resultImportDS.stderr);
  process.exit(resultImportDS.code);
}

// TODO: limit only to policies, rules and related assets (not all terms, categories, etc)
console.log("Importing all Information Governance Catalog assets...");
let cmdImportBG = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh glossary import" +
    " -u " + argv.deploymentUser +
    " -p " + argv.deploymentUserPassword +
    " -f '" + tmpPath + path.sep + baseFilename + ".xmi'" +
    " -format XMI -mergemethod mergeoverwrite";
if (argv.mapping) {
  cmdImportBG = cmdImportBG + " -map '" + argv.mapping + "'";
}
const resultImportBG = exec(cmdImportBG, {silent: true, "shell": "/bin/bash"});
if (resultImportBG.code !== 0) {
  console.error("ERROR importing IGC content:");
  console.error(resultImportBG.stderr);
  process.exit(resultImportBG.code);
}

if (bCreated) {
  rm('-rf', tmpPath);
} else {
  rm(tmpPath + path.sep + baseFilename + ".isx");
  rm(tmpPath + path.sep + baseFilename + ".xmi");
}
