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
 * @file Retrieve all of the metadata related to a particular data rule
 * @license Apache-2.0
 * @requires ibm-igc-rest
 * @requires yargs
 * @example
 * // retrieves all of the metadata related to all executable rules
 * ./getMetadataForExecutableDataRules.js -d hostname:9445 -u isadmin -p isadmin
 * @example
 * // retrieves all of the metadata related to the data rule named 'IndustryCodeMustExist'
 * ./getMetadataForExecutableDataRules.js -n 'IndustryCodeMustExist' -d hostname:9445 -u isadmin -p isadmin
 */

var igcrest = require('ibm-igc-rest');

// Command-line setup
var yargs = require('yargs');
var argv = yargs
    .usage('Usage: $0 -n <name> -d <host>:<port> -u <user> -p <password>')
    .option('n', {
      alias: 'name',
      describe: 'Name of the Data Rule',
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

// Base settings
var bContinueOnError = true;
var host_port = argv.domain.split(":");
var lastRefreshTime = argv.time;

igcrest.setAuth(argv.deploymentUser, argv.deploymentUserPassword);
igcrest.setServer(host_port[0], host_port[1]);

var receivedRuleName = argv.name;

var iaDataRuleQ = {
  "pageSize": "10000",
  "properties": [ "name", "implements_rules", "implements_rules.referencing_policies", "implements_rules.governs_assets", "implemented_bindings", "implemented_bindings.assigned_to_terms", "implemented_bindings.assigned_to_terms.stewards" ],
  "types": [ "data_rule" ]
};

if (receivedRuleName !== undefined) {
  iaDataRuleQ["where"] = {
    "operator": "and",
    "conditions":
    [
      {
        "property": "name",
        "operator": "=",
        "value": receivedRuleName
      }
    ]
  };
}

// NOTE: we are NOT retrieving the association to term from Info Gov Rule, and from term to information asset
// -- that might allow us to do the same for Data Rules executed via DataStage; however...
// In reality this probably doesn't work: the Term linked to the Info Gov Rule may actually link to a column in each of OS, IWH, DM, etc
// (and what we probably really want from DS-embedded checks is who owns the delivery / job that feeds into ours -- the overall system or file owner
// -- which we can then trace through lineage to see who is responsible for producing the input: may not be a source data issue, but a transforamtion / 
// processing issue)

igcrest.search(iaDataRuleQ, function (err, resSearch) {

  if (resSearch.items.length == 0) {
    console.warn("WARN: Did not find any Data Rules with the name '" + receivedRuleName + "'.");
  } else if (resSearch.items.length > 1) {
    console.warn("WARN: Found more than one Data Rule with the name '" + receivedRuleName +"'.");
  } else {

    for (var r = 0; r < resSearch.items.length; r++) {

      var rule = resSearch.items[r];
      var ruleName = rule._name;
      var policyDetails = rule["implements_rules.referencing_policies"].items;
      var infoGovRuleDetails = rule["implements_rules"].items;
      var termDetails = rule["implemented_bindings.assigned_to_terms"].items;
      var bindingDetails = rule["implemented_bindings"].items;
  
      var dqDimension = policyDetails[0]._name;
      var infoGovRuleName = infoGovRuleDetails[0]._name;
      var colName = bindingDetails[0]._name;
      var colRID = bindingDetails[0]._id;
  
      var aTerms = [];
      var aStewards = [];
      for (var i = 0; i < termDetails.length; i++) {
        aTerms.push(termDetails[i]._name);
        getDataOwners("term", termDetails[i]._id, function(err, aStewardsForATerm) {
          aStewards.push.apply(aStewards, aStewardsForATerm);
          outputRuleMetadata(ruleName, infoGovRuleName, dqDimension, colName, colRID, aTerms, aStewards);
        });
      }

    }

  }

});

var dsDataRuleQ = {
  "pageSize": "10000",
  "properties": [ "name", "implements_rules", "implements_rules.referencing_policies", "implements_rules.governs_assets", "job_or_container" ],
  "types": [ "stage" ],
  "where":
  {
    "operator": "and",
    "conditions":
    [
      {
        "property": "type.name",
        "operator": "=",
        "value": "IADataRule"
      }
    ]
  }
};

if (receivedRuleName !== undefined) {
  dsDataRuleQ.where.conditions.push({
    "property": "name",
    "operator": "=",
    "value": receivedRuleName
  });
}

igcrest.search(dsDataRuleQ, function (err, resSearch) {

  if (resSearch.items.length == 0) {
    console.warn("WARN: Did not find any Data Rule Stage with the name '" + receivedRuleName + "'.");
  } else if (resSearch.items.length > 1) {
    console.warn("WARN: Found more than one Data Rule Stage with the name '" + receivedRuleName +"'.");
  } else {

    for (var r = 0; r < resSearch.items.length; r++) {

      var rule = resSearch.items[r];
      var ruleName = rule._name;
      var policyDetails = rule["implements_rules.referencing_policies"].items;
      var infoGovRuleDetails = rule["implements_rules"].items;
      var governedAssets = rule["implements_rules.governs_assets"].items;
      var dqDimension = policyDetails[0]._name;
      var infoGovRuleName = infoGovRuleDetails[0]._name;

      var aStewards = [];
      getInputsForJob(rule["job_or_container"]._id, function(err, resAssets) {
        var assetRIDs = Object.keys(resAssets);
        var iAssetsProcessed = 0;
        for (var i = 0; i < assetRIDs.length; i++) {
          getDataOwners(resAssets[assetRIDs[i]], assetRIDs[i], function(errOwner, aStewardsForAsset) {
            iAssetsProcessed += 1;
            aStewards.push.apply(aStewards, aStewardsForAsset);
            if (iAssetsProcessed == assetRIDs.length) {
              outputDSRuleMetadata(ruleName, infoGovRuleName, dqDimension, resAssets, aStewards);
            }
          });
        }
      });
  
    }

  }

});

function getInputsForJob(jobRID, callback) {
  igcrest.getAssetPropertiesById(jobRID, "dsjob", ["reads_from_(design)", "reads_from_(operational)"], 100, true, function(err, resJob) {

    var errJob = err;
    var inputAssets = {};

    if (resJob === undefined) {
      errJob = "Unable to find job with RID = " + jobRID;
    } else {
      var inputAssetsDesign = resJob["reads_from_(design)"].items;
      var inputAssetsOperational = resJob["reads_from_(operational)"].items;
      for (var i = 0; i < inputAssetsDesign.length; i++) {
        var inputAssetDesign = inputAssetsDesign[i];
        inputAssets[inputAssetDesign._id] = inputAssetDesign._type;
      }
      for (var i = 0; i < inputAssetsOperational.length; i++) {
        var inputAssetOperational = inputAssetsOperational[i];
        inputAssets[inputAssetOperational._id] = inputAssetOperational._type;
      }
    }
    callback(errJob, inputAssets);
    return inputAssets;

  });
}

function getDataOwners(type, rid, callback) {
  igcrest.getAssetPropertiesById(rid, type, ["stewards"], 100, true, function(err, resAsset) {
    var errAsset = err;
    var aStewards = [];
    if (resAsset === undefined || (errAsset !== null && errAsset.startsWith("WARN: No assets found"))) {
      errAsset = "Unable to find a " + type + " with RID = " + rid;
    } else {
      var stewards = resAsset.stewards.items;
      for (var j = 0; j < stewards.length; j++) {
        aStewards.push(stewards[j]._name);
      }
    }
    callback(errAsset, aStewards);
    return aStewards;
  });
}

function outputRuleMetadata(ruleName, infoGovRuleName, dqDimension, colName, colRID, aTerms, aStewards) {
  console.log("Found the following for rule '" + ruleName + "':");
  console.log("  - Info gov rule   = " + infoGovRuleName);
  console.log("  - DQ dimension    = " + dqDimension);
  console.log("  - Bound column    = " + colName + " (" + colRID + ")");
  console.log("  - Related term(s) = " + aTerms);
  console.log("  - Data owner(s)   = " + aStewards);
}

function outputDSRuleMetadata(ruleName, infoGovRuleName, dqDimension, inputAssets, aStewards) {
  console.log("Found the following for rule '" + ruleName + "':");
  console.log("  - Info gov rule   = " + infoGovRuleName);
  console.log("  - DQ dimension    = " + dqDimension);
  console.log("  - Input(s)        = " + Object.keys(inputAssets));
  console.log("  - Input owner(s)  = " + aStewards);
}
