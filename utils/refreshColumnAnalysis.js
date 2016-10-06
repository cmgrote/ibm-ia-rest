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
 * @requires ibm-iis-kafka
 * @requires yargs
 * @example
 * // refreshes column analysis for all columns and file fields with results older than 24 hours, within the "Automated Profiling" project (by default)
 * ./refreshColumnAnalysis.js -t 1440 -d hostname:9445 -z hostname:52181 -u isadmin -p isadmin
 */

const iarest = require('ibm-ia-rest');
const commons = require('ibm-iis-commons');
const iiskafka = require('ibm-iis-kafka');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -n <name> -t <timeInMinutes> -d <host>:<port> -z <host>:<port> -u <user> -p <password>')
    .option('n', {
      alias: 'name',
      describe: 'Name of the Information Analyzer project',
      demand: true, requiresArg: true, type: 'string',
      default: "Automated Profiling"
    })
    .option('t', {
      alias: 'time',
      describe: 'Re-run analysis on any assets without results published in the last T minutes',
      requiresArg: true, type: 'number'
    })
    .env('DS')
    .option('z', {
      alias: 'zookeeper',
      describe: 'Host and port for Zookeeper connection to consume from Kafka',
      demand: true, requiresArg: true, type: 'string'
    })
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
  const hours = (lastRefreshTime / 60);
  const minutes = (lastRefreshTime % 60);
  staleBefore.setHours(now.getHours() - hours, now.getMinutes() - minutes);
  console.log("Refreshing any analysis last completed before: " + staleBefore);
}

const infosphereEventEmitter = new iiskafka.InfosphereEventEmitter(argv.zookeeper, 'automated-profiling-handler', false);

let tamsToSources = {};
let iTotalTAMs = 0;
const tamsProcessed = [];

iarest.getStaleAnalysisResults(projectName, staleBefore, function(errStale, aStaleSources) {
  handleError("getting stale analysis results", errStale);

  console.log("  running column analysis for " + aStaleSources.length + " sources.");
  iTotalTAMs = aStaleSources.length;
  iarest.runColumnAnalysisForDataSources(aStaleSources, function(errExec, tamsAnalyzed) {
    handleError("running column analysis", errExec);

    // Keep a copy of the TAMs we submitted for analysis so that we can track their completion via Kafka (for auto-publishing once completed)
    tamsToSources = tamsAnalyzed;
    recordCompletion(null);

  });

});

// TODO: we could also look into outputting additional progress information via these events (all of which have tamRid)
//      IA_DATAQUALITY_ANALYSIS_SUBMITTED
//      IA_COLUMN_ANALYSIS_STARTED_EVENT
//      IA_PROFILE_BATCH_COMPLETED_EVENT
//      IA_DATAQUALITY_ANALYSIS_STARTED_EVENT
//      IA_TABLE_RESULTS_PUBLISHED

infosphereEventEmitter.on('IA_DATAQUALITY_ANALYSIS_FINISHED_EVENT', publishAnalysis);
infosphereEventEmitter.on('IA_DATAQUALITY_ANALYSIS_FAILED_EVENT', logFailure);
infosphereEventEmitter.on('error', function(errMsg) {
  console.error("Received 'error' -- aborting process: " + errMsg);
  process.exit(1);
});
infosphereEventEmitter.on('end', function() {
  console.log("Event emitter stopped -- ending process.");
  process.exit();
});

function handleError(ctxMsg, errMsg) {
  if (typeof errMsg !== 'undefined' && errMsg !== null) {
    console.error("Failed " + ctxMsg + " -- " + errMsg);
    process.exit(1);
  }
}

function logProgress(msg) {
  console.log("  [" + (tamsProcessed.length + 1) + "/" + iTotalTAMs + "] " + msg);
}

function publishAnalysis(infosphereEvent, eventCtx, commitCallback) {
  const projectRid = infosphereEvent.projectRid;
  const tamRid = infosphereEvent.tamRid;
  if (tamsToSources.hasOwnProperty(tamRid)) {
    const objectIdentity = tamsToSources[tamRid].identity;
    logProgress("publishing analysis results for " + objectIdentity);
    const aTAMs = [];
    aTAMs.push(tamRid);
    iarest.publishResultsForDataSources(projectRid, aTAMs, function(errPublish) {
      handleError("publishing analysis results", errPublish);
      commitCallback(eventCtx);
      recordCompletion(tamRid);
    });
  } else {
    commitCallback(eventCtx);
  }
}

function logFailure(infosphereEvent, eventCtx, commitCallback) {
  const tamRid = infosphereEvent.tamRid;
  if (tamsToSources.hasOwnProperty(tamRid)) {
    const objectIdentity = tamsToSources[tamRid].identity;
    logProgress("analysis failed for " + objectIdentity);
    commitCallback(eventCtx);
    recordCompletion(tamRid);
  } else {
    commitCallback(eventCtx);
  }
}

function recordCompletion(tamRid) {
  if (tamRid !== null) {
    tamsProcessed.push(tamRid);
  }
  if (iTotalTAMs === tamsProcessed.length) {
    infosphereEventEmitter.emit('end');
  }
}
