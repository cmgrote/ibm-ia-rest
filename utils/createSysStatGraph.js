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
 * @requires shelljs
 * @requires moment
 * @requires yargs
 */

const shell = require('shelljs');
const moment = require('moment');
const fs = require('fs');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
    .usage('Usage: $0 -f <file> -s <host>')
    .option('f', {
      alias: 'file',
      describe: 'Name of the results file for which to create a graph',
      demand: true, requiresArg: true, type: 'string'
    })
    .option('s', {
      alias: 'host',
      describe: 'Name of the host on which this is running',
      demand: true, requiresArg: true, type: 'string',
      default: process.env.HOSTNAME
    })
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;

const metrics = {
  "paging": "-B",
  "io": "-b",
  "network": "-n DEV",
  "cpu": "-u ALL",
  "load": "-q",
  "memory": "-r ALL",
  "swap": "-S",
  "swappaging": "-W",
  "switching": "-w"
};

const ruleDetails = fs.readFileSync(argv.file, 'utf8').split(",");
const projName = ruleDetails[0];
const ruleName = ruleDetails[1];
const dRuleExecStart = moment(ruleDetails[2]);
const dRuleExecEnd = moment(ruleDetails[3]);

const aMetricNames = Object.keys(metrics);
for (let i = 0; i < aMetricNames.length; i++) {
  const metricName = aMetricNames[i];
  if (metrics.hasOwnProperty(metricName)) {
    createSysstatGraph(metricName, metrics[metricName]);
  }
}

function getBaseFilename() {
  return "/data/ruleTest__" + projName.replace(/ /g, "_") + "_" + ruleName.replace(/ /g, "_") + "_" + dRuleExecStart.toISOString();
}

function createSysstatGraph(metricName, metricCode) {
  console.log("Saving SVG for " + metricName);
  const filename = getBaseFilename() + "__" + argv.host + "__sar_" + metricName + ".svg";
  console.log(" ... writing: " + filename);
  const cmdSadf = "/usr/local/bin/sadf -g" +
      " -s " + dRuleExecStart.format("HH:mm") + ":00" +
      " -e " + dRuleExecEnd.format("HH:mm") + ":59" +
      " -- " + metricCode + " > " + filename;
  console.log("  --> " + cmdSadf);
  const result = shell.exec(cmdSadf, {silent: true, "shell": "/bin/bash"});
  if (result.code !== 0) {
    console.error("ERROR outputting sysstat graph:");
    console.error(result.stderr);
    process.exit(result.code);
  }
}
