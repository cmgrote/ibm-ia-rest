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
 * @file Executes the specified rule and determines performance-related information for the execution
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires ibm-iis-commons
 * @requires shelljs
 * @requires moment
 * @requires yargs
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const shell = require('shelljs');
const moment = require('moment');
const fs = require('fs');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -p <project> -r <ruleName> -a <authorisationFile>')
    .option('p', {
      alias: 'project',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('r', {
      alias: 'rule',
      describe: 'Name of the rule to performance test',
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

const restConnect = new commons.RestConnection("isadmin", "isadmin", envCtx.domainHost, envCtx.domainPort);
iarest.setConnection(restConnect);

console.log("Executing Information Analyzer rule '" + argv.project +  "::" + argv.rule + "'...");
let cmdExecRule = envCtx.asbhome + "/bin/IAJob.sh" +
    " -user " + envCtx.username +
    " -password '" + envCtx.password + "'" +
    " -isHost " + envCtx.domainHost +
    " -port " + envCtx.domainPort +
    " -run '" + argv.project + "' '" + argv.rule + "'";
console.log("  --> " + cmdExecRule);
const dRuleExecStart = moment();
const result = shell.exec(cmdExecRule, {silent: true, "shell": "/bin/bash"});
const dRuleExecEnd = moment();
if (result.code !== 0) {
  console.error("ERROR executing IA rule:");
  console.error(result.stderr);
  process.exit(result.code);
}

iarest.getRuleExecutionResults(argv.project, argv.rule, true, function(err, aStats) {
  handleError("retrieving rule execution results", err);
  const stat = aStats[0];
  const filename = getBaseFilename() + "__stats.csv";
  const data = argv.project + "," + argv.rule + "," + moment(dRuleExecStart).toISOString() + "," + moment(dRuleExecEnd).toISOString() + "," + (dRuleExecEnd - dRuleExecStart) + "," + stat.dStart + "," + stat.dEnd + "," + (moment(stat.dEnd) - moment(stat.dStart)) + "," + stat.numFailed + "," + stat.numTotal + "\n";
  console.log(" ... writing: " + filename);
  fs.writeFileSync(filename, data, 'utf8');
  cleanUp();
});

function cleanUp() {
  console.log("Cleaning out exceptions table for rule execution.");
  const cmdClean = envCtx.ishome + "/ASBServer/bin/IAAdmin.sh" +
      " -user " + envCtx.username +
      " -password '" + envCtx.password + "'" +
      " -url https://" + envCtx.domainHost + ":" + envCtx.domainPort +
      " -deleteOutputTable" +
      " -projectName '" + argv.project + "'" +
      " -ruleName '" + argv.rule + "'" +
      " -keepLastRuns 0";
  console.log("  --> " + cmdClean);
  const result = shell.exec(cmdClean, {silent: true, "shell": "/bin/bash"});
  if (result.code !== 0) {
    console.error("ERROR cleaning up output table of IA rule:");
    console.error(result.stderr);
  }
}

function handleError(ctxMsg, errMsg) {
  if (typeof errMsg !== 'undefined' && errMsg !== null) {
    console.error("Failed " + ctxMsg + " -- " + errMsg);
    process.exit(1);
  }
}

function getBaseFilename() {
  return "/data/ruleTest__" + argv.project.replace(/ /g, "_") + "_" + argv.rule.replace(/ /g, "_") + "_" + dRuleExecStart.toISOString();
}
