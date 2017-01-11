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
 * @file Extract all rule-related assets from an environment
 * @license Apache-2.0
 * @requires ibm-iis-commons
 * @requires shelljs
 * @requires yargs
 * @example
 * // exports all rule-related assets from the "DQ Experiments" Information Analyzer project and "DQ" DataStage project into /tmp/extract.tgz
 * ./exportAllRuleAssets.js -i 'DQ Experiments' -d 'DQ' -f /tmp/extract
 */

const commons = require('ibm-iis-commons');
const shell = require('shelljs');
const path = require('path');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -i <iaProjectName> -d <dsProjectName> -f <path> -a <authorisationFile>')
    .option('i', {
      alias: 'ianame',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('d', {
      alias: 'dsname',
      describe: 'Name of the DataStage project',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('f', {
      alias: 'filepath',
      describe: 'Path to the file in which to save assets',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('a', {
      alias: 'authfile',
      describe: 'Authorisation file containing environment context',
      requiresArg: true, type: 'string'
    })
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;

const envCtx = new commons.EnvironmentContext();
if (argv.authfile !== undefined && argv.authfile !== "") {
  envCtx.authFile = argv.authfile;
}

console.log("Extracting all Information Analyzer assets from '" + argv.ianame + "'...");
const cmdExportIA = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh export" +
    " -af " + envCtx.authFile +
    " -ar '" + argv.filepath + ".isx'" +
    " -up -ia '-projects \"\\\\" + argv.ianame + "\\\\\"" +
    " -includeDataClasses -includeCommonMetadata -includeProjectRoles -includeReports'";
const resultExportIA = shell.exec(cmdExportIA, {silent: true, "shell": "/bin/bash"});
if (resultExportIA.code !== 0) {
  console.error("ERROR exporting IA content:");
  console.error(resultExportIA.stderr);
  process.exit(resultExportIA.code);
}

// TODO: limit only to policies, rules and related assets (not all terms, categories, etc)
console.log("Extracting all Information Governance Catalog assets...");
const cmdExportBG = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh glossary export" +
    " -af " + envCtx.authFile +
    " -f '" + argv.filepath + ".xmi'" +
    " -format XMI -allpoliciesrules -includeassignedassets -includestewardship -includelabeledassets -includeTermHistory";
const resultExportBG = shell.exec(cmdExportBG, {silent: true, "shell": "/bin/bash"});
if (resultExportBG.code !== 0) {
  console.error("ERROR exporting IGC content:");
  console.error(resultExportBG.stderr);
  process.exit(resultExportBG.code);
}

// TODO: limit only to DataStage jobs that include Data Rules Stages (?)
console.log("Extracting all DataStage assets from '" + argv.dsname + "'...");
const cmdExportDS = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh export" +
    " -af " + envCtx.authFile +
    " -ar '" + argv.filepath + ".isx'" +
    " -up -ds '" + envCtx.engine + "/" + argv.dsname + "/*/*.* -incdep'";
const resultExportDS = shell.exec(cmdExportDS, {silent: true, "shell": "/bin/bash"});
if (resultExportDS.code !== 0) {
  console.error("ERROR exporting DS content:");
  console.error(resultExportDS.stderr);
  process.exit(resultExportDS.code);
}

const cmdTGZ = "tar zcv -C " + path.dirname(argv.filepath) + " -f '" + argv.filepath + ".tgz' '" + path.posix.basename(argv.filepath) + ".isx' '" + path.posix.basename(argv.filepath) + ".xmi'";
const resultTGZ = shell.exec(cmdTGZ, {silent: true, "shell": "/bin/bash"});
if (resultTGZ.code !== 0) {
  console.error("ERROR creating single bundle TGZ:");
  console.error(resultTGZ.stderr);
  process.exit(resultTGZ.code);
}

shell.rm(argv.filepath + ".isx");
shell.rm(argv.filepath + ".xmi");
