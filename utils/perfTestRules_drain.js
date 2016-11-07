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
 * @file Drains any left-over performance statistics from previous executions
 * @license Apache-2.0
 * @requires ibm-ia-rest
 * @requires ibm-iis-commons
 * @requires ibm-iis-kafka
 * @requires shelljs
 * @requires moment
 * @requires yargs
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const iiskafka = require('ibm-iis-kafka');
const shell = require('shelljs');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -z <host>:<port> [-a <authorisationFile>]')
    .option('z', {
      alias: 'zookeeper',
      describe: 'Host and port for Zookeeper connection to consume from Kafka',
      demand: true, requiresArg: true, type: 'string',
      default: "cgrotedl.demos.demoibm.com:52181"
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

const infosphereEventEmitter = new iiskafka.InfosphereEventEmitter(argv.zookeeper, 'ia-perf-test', false);

infosphereEventEmitter.on('NEW_EXCEPTIONS_EVENT', drainExecution);
infosphereEventEmitter.on('IA_DATARULE_FAILED_EVENT', drainExecutionByCommit);
infosphereEventEmitter.on('error', function(errMsg) {
  console.error("Received 'error' -- aborting process: " + errMsg);
  process.exit(1);
});
infosphereEventEmitter.on('end', function() {
  console.log("Event emitter stopped -- ending process.");
  process.exit();
});

function getRuleIdentityString(projectName, ruleName) {
  return projectName + "::" + ruleName;
}

function drainExecution(infosphereEvent, eventCtx, commitCallback) {
  const ruleName = infosphereEvent.exceptionSummaryName;
  const projName = infosphereEvent.projectName;
  const ruleId = getRuleIdentityString(projName, ruleName);
  console.log("Found left-over execution -- cleaning it: " + ruleId);
  cleanUp({"project": projName, "rule": ruleName});
  commitCallback(eventCtx);
}

function drainExecutionByCommit(InfosphereEvent, eventCtx, commitCallback) {
  commitCallback(eventCtx);
}

function cleanUp(execObj) {
  console.log("Cleaning out exceptions table for rule execution.");
  const cmdCleanData = envCtx.ishome + "/ASBServer/bin/IAAdmin.sh" +
      " -user " + envCtx.username +
      " -password '" + envCtx.password + "'" +
      " -url https://" + envCtx.domainHost + ":" + envCtx.domainPort +
      " -deleteOutputTable" +
      " -projectName '" + execObj.project + "'" +
      " -ruleName '" + execObj.rule + "'" +
      " -keepLastRuns 0";
  console.log("  --> " + cmdCleanData);
  const resultData = shell.exec(cmdCleanData, {silent: true, "shell": "/bin/bash"});
  if (resultData === null || resultData.code !== 0) {
    console.error("ERROR cleaning up output table of IA rule:");
    console.error(resultData.stderr);
  }
  console.log("Deleting rule execution history (to avoid false positives later).");
  const cmdCleanStats = envCtx.ishome + "/ASBServer/bin/IAAdmin.sh" +
    " -user " + envCtx.username +
      " -password '" + envCtx.password + "'" +
      " -url https://" + envCtx.domainHost + ":" + envCtx.domainPort +
      " -deleteExecutionHistory" +
      " -projectName '" + execObj.project + "'" +
      " -ruleName '" + execObj.rule + "'" +
      " -keepLastRuns 0";
  console.log("  --> " + cmdCleanStats);
  const resultStats = shell.exec(cmdCleanStats, {silent: true, "shell": "/bin/bash"});
  if (resultStats === null || resultStats.code !== 0) {
    console.error("ERROR cleaning up output table of IA rule:");
    console.error(resultStats.stderr);
  }
}
