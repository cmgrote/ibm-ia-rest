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
 * @file Retrieve all of the metadata related to a particular data rule
 * @license Apache-2.0
 * @requires ibm-igc-rest
 * @requires ibm-iis-commons
 * @requires yargs
 * @example
 * // retrieves all of the metadata related to all executable rules
 * ./getMetadataForExecutableDataRules.js -d hostname:9445 -u isadmin -p isadmin
 * @example
 * // retrieves all of the metadata related to the data rule named 'IndustryCodeMustExist'
 * ./getMetadataForExecutableDataRules.js -n 'IndustryCodeMustExist' -d hostname:9445 -u isadmin -p isadmin
 */

const igcrest = require('ibm-igc-rest');
const commons = require('ibm-iis-commons');

// Command-line setup
const yargs = require('yargs');
const argv = yargs
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
const host_port = argv.domain.split(":");

const restConnect = new commons.RestConnection(argv.deploymentUser, argv.deploymentUserPassword, host_port[0], host_port[1]);
igcrest.setConnection(restConnect);

const receivedRuleName = argv.name;

const iaDataRuleQ = {
  "pageSize": "10000",
  "properties": [ "name", "implements_rules", "implements_rules.referencing_policies", "implements_rules.governs_assets", "implemented_bindings", "implemented_bindings.assigned_to_terms", "implemented_bindings.assigned_to_terms.stewards" ],
  "types": [ "data_rule" ]
};

if (receivedRuleName !== undefined) {
  iaDataRuleQ.where = {
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

function processAllCollectedDataForRule(err, aStewardsForOneObject, iProcessed, iFound, passthru, aStewards) {
  iProcessed++;
  aStewards.push.apply(aStewards, aStewardsForOneObject);
  if (iProcessed === iFound) {
    outputRuleMetadata(passthru.ruleName, passthru.infoGovRuleName, passthru.dqDimension, passthru.aColNames, passthru.aColRIDs, passthru.aTerms, aStewards);
  }
}

// NOTE: we are NOT retrieving the association to term from Info Gov Rule, and from term to information asset
// -- that might allow us to do the same for Data Rules executed via DataStage; however...
// In reality this probably doesn't work: the Term linked to the Info Gov Rule may actually link to a column in each of OS, IWH, DM, etc
// (and what we probably really want from DS-embedded checks is who owns the delivery / job that feeds into ours -- the overall system or file owner
// -- which we can then trace through lineage to see who is responsible for producing the input: may not be a source data issue, but a transforamtion / 
// processing issue)

igcrest.search(iaDataRuleQ, function (err, resSearch) {

  if (resSearch.items.length === 0) {
    if (receivedRuleName === undefined) {
      console.warn("WARN: Did not find any Data Rules defined.");
    } else {
      console.warn("WARN: Did not find any Data Rules with the name '" + receivedRuleName + "'.");
    }
  } else {

    for (let r = 0; r < resSearch.items.length; r++) {

      const rule = resSearch.items[r];
      const ruleName = rule._name;
      const policyDetails = rule["implements_rules.referencing_policies"].items;
      const infoGovRuleDetails = rule.implements_rules.items;
      const termDetails = rule["implemented_bindings.assigned_to_terms"].items;
      const governedAssets = rule["implements_rules.governs_assets"].items;
      const bindingDetails = rule.implemented_bindings.items;

      if (policyDetails.length === 0 || infoGovRuleDetails.length === 0 || bindingDetails.length === 0) {
        console.warn("WARN: Rule '" + ruleName + "' is either not bound to an implementation or is missing other expected relationships.");
      } else {
        const dqDimension = policyDetails[0]._name;
        const infoGovRuleName = infoGovRuleDetails[0]._name;
        const aColNames = [];
        const aColRIDs = [];
        for (let i = 0; i < bindingDetails.length; i++) {
          aColNames.push(bindingDetails[i]._name);
          aColRIDs.push(bindingDetails[i]._id);
        }
//        var colName = bindingDetails[0]._name;
//        var colRID = bindingDetails[0]._id;
    
        const aTerms = [];
        const aStewards = [];
        let iFoundTerms = 0;
        const iProcessedTerms = 0;
        if (termDetails.length === 0) {
          for (let i = 0; i < governedAssets.length; i++) {
            if (governedAssets[i]._type === "term") {
              iFoundTerms++;
              aTerms.push(governedAssets[i]._name);
              const objDetails = {
                'ruleName': ruleName,
                'infoGovRuleName': infoGovRuleName,
                'dqDimension': dqDimension,
                'aColNames': aColNames,
                'aColRIDs': aColRIDs,
                'aTerms': aTerms
              };
              getDataOwners("term", governedAssets[i]._id, objDetails, iProcessedTerms, iFoundTerms, aStewards, processAllCollectedDataForRule);
            }
          }
        } else {
          for (let i = 0; i < termDetails.length; i++) {
            iFoundTerms++;
            aTerms.push(termDetails[i]._name);
            const objDetails = {
              'ruleName': ruleName,
              'infoGovRuleName': infoGovRuleName,
              'dqDimension': dqDimension,
              'aColNames': aColNames,
              'aColRIDs': aColRIDs,
              'aTerms': aTerms
            };
            getDataOwners("term", termDetails[i]._id, objDetails, iProcessedTerms, iFoundTerms, aStewards, processAllCollectedDataForRule);
          }
        }
      }

    }

  }

});

const dsDataRuleQ = {
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

function processAllCollectedDataForRuleDS(err, aStewardsForOneObject, iProcessed, iFound, passthru, aStewards) {
  iProcessed++;
  aStewards.push.apply(aStewards, aStewardsForOneObject);
  if (iProcessed === iFound) {
    outputDSRuleMetadata(passthru.ruleName, passthru.infoGovRuleName, passthru.dqDimension, passthru.resAssets, aStewards);
  }
}

function processJobInputs(err, resAssets, ruleName, infoGovRuleName, dqDimension, aStewards) {
  const assetRIDs = Object.keys(resAssets);
  let iAssetsProcessed = 0;
  for (let i = 0; i < assetRIDs.length; i++) {
    const objDetails = {
      'ruleName': ruleName,
      'infoGovRuleName': infoGovRuleName,
      'dqDimension': dqDimension,
      'resAssets': resAssets
    };
    getDataOwners(resAssets[assetRIDs[i]], assetRIDs[i], objDetails, iAssetsProcessed, assetRIDs.length, aStewards, processAllCollectedDataForRuleDS);
  }
}

igcrest.search(dsDataRuleQ, function (err, resSearch) {

  if (resSearch.items.length === 0) {
    console.warn("WARN: Did not find any Data Rule Stage with the name '" + receivedRuleName + "'.");
  } else {

    for (let r = 0; r < resSearch.items.length; r++) {

      const rule = resSearch.items[r];
      const ruleName = rule._name;
      const policyDetails = rule["implements_rules.referencing_policies"].items;
      const infoGovRuleDetails = rule.implements_rules.items;
      //const governedAssets = rule["implements_rules.governs_assets"].items;

      if (policyDetails.length === 0 || infoGovRuleDetails.length === 0) {
        console.warn("WARN: DataStage-embedded rule '" + ruleName + "' is missing expected business metadata relationships.");
      } else {
        const dqDimension = policyDetails[0]._name;
        const infoGovRuleName = infoGovRuleDetails[0]._name;
        const aStewards = [];
        getInputsForJob(rule.job_or_container._id, ruleName, infoGovRuleName, dqDimension, aStewards, processJobInputs);
      }
  
    }

  }

});

function getInputsForJob(jobRID, ruleName, infoGovRuleName, dqDimension, aStewards, callback) {
  igcrest.getAssetPropertiesById(jobRID, "dsjob", ["reads_from_(design)", "reads_from_(operational)"], 100, true, function(err, resJob) {

    let errJob = err;
    const inputAssets = {};

    if (resJob === undefined) {
      errJob = "Unable to find job with RID = " + jobRID;
    } else {
      const inputAssetsDesign = resJob["reads_from_(design)"].items;
      const inputAssetsOperational = resJob["reads_from_(operational)"].items;
      for (let i = 0; i < inputAssetsDesign.length; i++) {
        const inputAssetDesign = inputAssetsDesign[i];
        inputAssets[inputAssetDesign._id] = inputAssetDesign._type;
      }
      for (let i = 0; i < inputAssetsOperational.length; i++) {
        const inputAssetOperational = inputAssetsOperational[i];
        inputAssets[inputAssetOperational._id] = inputAssetOperational._type;
      }
    }
    return callback(errJob, inputAssets, ruleName, infoGovRuleName, dqDimension, aStewards);

  });
}

function getDataOwners(type, rid, passthru, iProcessed, iFound, aStewardsSoFar, callback) {
  igcrest.getAssetPropertiesById(rid, type, ["stewards"], 100, true, function(err, resAsset) {
    let errAsset = err;
    const aStewardsForAsset = [];
    if (resAsset === undefined || (errAsset !== null && errAsset.startsWith("WARN: No assets found"))) {
      errAsset = "Unable to find a " + type + " with RID = " + rid;
    } else {
      const stewards = resAsset.stewards.items;
      for (let j = 0; j < stewards.length; j++) {
        aStewardsForAsset.push(stewards[j]._name);
      }
    }
    return callback(errAsset, aStewardsForAsset, iProcessed, iFound, passthru, aStewardsSoFar);
  });
}

function outputRuleMetadata(ruleName, infoGovRuleName, dqDimension, colNames, colRIDs, aTerms, aStewards) {
  console.log("Found the following for rule '" + ruleName + "':");
  console.log("  - Info gov rule   = " + infoGovRuleName);
  console.log("  - DQ dimension    = " + dqDimension);
  console.log("  - Bound column    = " + colNames + " (" + colRIDs + ")");
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
