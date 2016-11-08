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
 * @requires ibm-iis-kafka
 * @requires shelljs
 * @requires moment
 * @requires yargs
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const iiskafka = require('ibm-iis-kafka');
const shell = require('shelljs');
const moment = require('moment');
const fs = require('fs');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -f <file> -z <host>:<port> [-a <authorisationFile>]')
    .option('f', {
      alias: 'file',
      describe: 'Filename containing JSON of rules to execute',
      demand: true, requiresArg: true, type: 'string'
    })
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

infosphereEventEmitter.on('NEW_EXCEPTIONS_EVENT', closeExecution);
infosphereEventEmitter.on('IA_DATARULE_FAILED_EVENT', cancelExecution);
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

const ruleExecutions = JSON.parse(fs.readFileSync(argv.file, 'utf8'));

const rulesStarted = {};
const rulesProcessed = [];
const ruleIds = Object.keys(ruleExecutions);
const iTotalRules = ruleIds.length;

let currentRule = 0;
runNextRule(currentRule++);

function runNextRule(index) {

  if (index < iTotalRules) {
    const ruleId = ruleIds[index];
    const projName = ruleId.substring(0, ruleId.indexOf("::"));
    const ruleName = ruleId.substring(ruleId.indexOf("::") + 2);
    ruleExecutions[ruleId].project = projName;
    ruleExecutions[ruleId].rule = ruleName;
    console.log("Executing Information Analyzer rule '" + projName +  "::" + ruleName + "'...");
    let cmdExecRule = envCtx.asbhome + "/bin/IAJob.sh" +
        " -user " + envCtx.username +
        " -password '" + envCtx.password + "'" +
        " -isHost " + envCtx.domainHost +
        " -port " + envCtx.domainPort +
        " -run '" + projName + "' '" + ruleName + "'";
    console.log("  --> " + cmdExecRule);
    ruleExecutions[ruleId].mRuleCmdStarted = moment();
    const result = shell.exec(cmdExecRule, {silent: true, "shell": "/bin/bash", async: true});
    rulesStarted[ruleId] = true;
    result.on('close', (code) => {
      ruleExecutions[ruleId].mRuleCmdReturned = moment();
      ruleExecutions[ruleId].exitCode = code;
      if (code !== 0) {
        console.error("ERROR executing IA rule: " + ruleId);
      }
    });
  }

}

function handleError(ctxMsg, errMsg) {
  if (typeof errMsg !== 'undefined' && errMsg !== null) {
    console.error("Failed " + ctxMsg + " -- " + errMsg);
    process.exit(1);
  }
}

function getBaseFilename(projectName, ruleName) {
  return "/data/ruleTest__" + projectName.replace(/ /g, "_") + "_" + ruleName.replace(/ /g, "_");
}

function handleError(ctxMsg, errMsg) {
  if (typeof errMsg !== 'undefined' && errMsg !== null) {
    console.error("Failed " + ctxMsg + " -- " + errMsg);
    process.exit(1);
  }
}

function checkAndOutputResults(execObj) {
  // Only output the final information when both the command has returned and the final event has been raised
  if (execObj.hasOwnProperty('mRuleCmdReturned') && execObj.hasOwnProperty('mFinalEventRaised')) {
    const filename = getBaseFilename(execObj.project, execObj.rule) + "__stats.csv";
    const data = execObj.project + "," + execObj.rule + "," + execObj.mRuleCmdStarted.toISOString() + "," + execObj.mRuleCmdReturned.toISOString() + "," + execObj.mFinalEventRaised.toISOString() + "," + (execObj.mRuleCmdReturned - execObj.mRuleCmdStarted) + "," + (execObj.mFinalEventRaised - execObj.mRuleCmdStarted) + "," + execObj.mRecordedStart.toISOString() + "," + execObj.mRecordedEnd.toISOString() + "," + (execObj.mRecordedEnd - execObj.mRecordedStart) + "," + execObj.numFailed + "," + execObj.numTotal + "\n";
    fs.writeFileSync(filename, data, 'utf8');
    cleanUp(execObj);
    recordCompletion(execObj.rule);
    runNextRule(currentRule++);
  }
}

function recordCompletion(ruleId) {
  if (ruleId !== null) {
    rulesProcessed.push(ruleId);
  }
  if (iTotalRules === rulesProcessed.length) {
    infosphereEventEmitter.emit('end');
  }
}

function cancelExecution(infosphereEvent, eventCtx, commitCallback) {
  console.error("ERROR: Execution of last rule failed.");
  const ruleId = ruleIds[currentRule];
  if (rulesStarted.hasOwnProperty(ruleId)) {
    console.error(" ... Attempting to close and clean the failed rule.");
    const execObj = ruleExecutions[ruleId];
    execObj.mFinalEventRaised = moment();
    const filename = getBaseFilename(execObj.project, execObj.rule) + "__failed.csv";
    const data = execObj.project + "," + execObj.rule + "," + execObj.mRuleCmdStarted.toISOString() + "," + execObj.mRuleCmdReturned.toISOString() + "," + execObj.mFinalEventRaised.toISOString() + "," + (execObj.mRuleCmdReturned - execObj.mRuleCmdStarted) + "," + (execObj.mFinalEventRaised - execObj.mRuleCmdStarted) + "," + execObj.mRecordedStart.toISOString() + "," + execObj.mRecordedEnd.toISOString() + "," + (execObj.mRecordedEnd - execObj.mRecordedStart) + ",-1,-1\n";
    fs.writeFileSync(filename, data, 'utf8');
    cleanUp(execObj);
    recordCompletion(execObj.rule);
    commitCallback(eventCtx);
    runNextRule(currentRule++);
  } else {
    console.log("Found execution that we were not tracking -- cleaning it: " + ruleId);
    const projName = ruleId.substring(0, ruleId.indexOf("::"));
    const ruleName = ruleId.substring(ruleId.indexOf("::") + 2);
    cleanUp({"project": projName, "rule": ruleName});
    commitCallback(eventCtx);
  }
}

function closeExecution(infosphereEvent, eventCtx, commitCallback) {
  const ruleName = infosphereEvent.exceptionSummaryName;
  const projName = infosphereEvent.projectName;
  const ruleId = getRuleIdentityString(projName, ruleName);
  if (rulesStarted.hasOwnProperty(ruleId)) {
    const execObj = ruleExecutions[ruleId];
    execObj.mFinalEventRaised = moment();
    iarest.getRuleExecutionResults(projName, ruleName, true, function(err, aStats) {
      console.log("Closing execution for: " + ruleId);
      handleError("retrieving rule execution results", err);
      const stat = aStats[0];
      if (typeof stat === 'undefined' || stat === null) {
        console.log("ERROR: No stat object for " + ruleId + "!  Waiting 5 seconds and retrying...");
        setTimeout(closeExecution, 5000, infosphereEvent, eventCtx, commitCallback);
      } else {
        execObj.mRecordedStart = moment(stat.dStart);
        execObj.mRecordedEnd = moment(stat.dEnd);
        execObj.numFailed = stat.numFailed;
        execObj.numTotal = stat.numTotal;
        checkAndOutputResults(execObj);
        commitCallback(eventCtx);
      }
    });
  } else {
    console.log("Found execution that we were not tracking -- cleaning it: " + ruleId);
    cleanUp({"project": projName, "rule": ruleName});
    commitCallback(eventCtx);
  }
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
  if (resultData.code !== 0) {
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
  if (resultStats.code !== 0) {
    console.error("ERROR cleaning up output table of IA rule:");
    console.error(resultStats.stderr);
  }
}
