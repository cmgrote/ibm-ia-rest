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
 * @file Extract all rule-related assets from an environment
 * @license Apache-2.0
 * @requires shelljs
 * @requires yargs
 * @example
 * // exports all rule-related assets from the "DQ Experiments" Information Analyzer project and "FWK" DataStage project into /tmp/extract.tgz
 * ./exportAllRuleAssets.js -i 'DQ Experiments' -d 'FWK' -e ENGINE.HOST.NAME -f /tmp/extract -u isadmin -p isadmin
 */

require('shelljs/global');
const path = require('path');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -i <iaProjectName> -d <dsProjectName> -e <engineTierFQDN> -f <path> -u <user> -p <password>')
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

console.log("Extracting all Information Analyzer assets from '" + argv.ianame + "'...");
var cmd = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh export"
        + " -u " + argv.deploymentUser
        + " -p " + argv.deploymentUserPassword
        + " -ar '" + argv.filepath + ".isx'"
        + " -up -ia '-projects \"\\\\" + argv.ianame + "\\\\\""
        + " -includeDataClasses -includeCommonMetadata -includeProjectRoles -includeReports'";
var result = exec(cmd, {silent: true, "shell": "/bin/bash"});
if (result.code !== 0) {
  console.error("ERROR exporting IA content:");
  console.error(result.stderr);
  process.exit(result.code);
}

// TODO: limit only to policies, rules and related assets (not all terms, categories, etc)
console.log("Extracting all Information Governance Catalog assets...");
cmd = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh glossary export"
    + " -u " + argv.deploymentUser
    + " -p " + argv.deploymentUserPassword
    + " -f '" + argv.filepath + ".xmi'"
    + " -format XMI -allpoliciesrules -includeassignedassets -includestewardship -includelabeledassets -includeTermHistory";
result = exec(cmd, {silent: true, "shell": "/bin/bash"});
if (result.code !== 0) {
  console.error("ERROR exporting IGC content:");
  console.error(result.stderr);
  process.exit(result.code);
}

// TODO: limit only to DataStage jobs that include Data Rules Stages (?)
console.log("Extracting all DataStage assets from '" + argv.dsname + "'...");
cmd = "/opt/IBM/InformationServer/Clients/istools/cli/istool.sh export"
    + " -u " + argv.deploymentUser
    + " -p " + argv.deploymentUserPassword
    + " -ar '" + argv.filepath + ".isx'"
    + " -up -ds '" + argv.engine + "/" + argv.dsname + "/*/*.* -incdep'";
result = exec(cmd, {silent: true, "shell": "/bin/bash"});
if (result.code !== 0) {
  console.error("ERROR exporting DS content:");
  console.error(result.stderr);
  process.exit(result.code);
}

cmd = "tar zcv -C " + path.dirname(argv.filepath) + " -f '" + argv.filepath + ".tgz' '" + path.posix.basename(argv.filepath) + ".isx' '" + path.posix.basename(argv.filepath) + ".xmi'";
result = exec(cmd, {silent: true, "shell": "/bin/bash"});
if (result.code !== 0) {
  console.error("ERROR creating single bundle TGZ:");
  console.error(result.stderr);
  process.exit(result.code);
}

rm(argv.filepath + ".isx");
rm(argv.filepath + ".xmi");
