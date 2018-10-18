/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RestrictedConfigChecker {
  private final static Logger LOG = LoggerFactory.getLogger(RestrictedConfigChecker.class);
  private final List<ConfVars> restrictedHiveConf = new ArrayList<>();
  private final List<String> restrictedNonHiveConf = new ArrayList<>();
  private final HiveConf initConf;

  RestrictedConfigChecker(HiveConf initConf) {
    this.initConf = initConf;
    String[] restrictedConfigs = HiveConf.getTrimmedStringsVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_SESSION_RESTRICTED_CONFIGS);
    if (restrictedConfigs == null || restrictedConfigs.length == 0) return;
    HashMap<String, ConfVars> confVars = HiveConf.getOrCreateReverseMap();
    for (String confName : restrictedConfigs) {
      if (confName == null || confName.isEmpty()) continue;
      confName = confName.toLowerCase();
      ConfVars cv = confVars.get(confName);
      if (cv != null) {
        restrictedHiveConf.add(cv);
      } else {
        LOG.warn("A restricted config " + confName + " is not recognized as a Hive setting.");
        restrictedNonHiveConf.add(confName);
      }
    }
  }

  public void validate(HiveConf conf) throws HiveException {
    for (ConfVars var : restrictedHiveConf) {
      String userValue = HiveConf.getVarWithoutType(conf, var),
          serverValue = HiveConf.getVarWithoutType(initConf, var);
      // Note: with some trickery, we could add logic for each type in ConfVars; for now the
      // potential spurious mismatches (e.g. 0 and 0.0 for float) should be easy to work around.
      validateRestrictedConfigValues(var.varname, userValue, serverValue);
    }
    for (String var : restrictedNonHiveConf) {
      String userValue = conf.get(var), serverValue = initConf.get(var);
      validateRestrictedConfigValues(var, userValue, serverValue);
    }
  }

  private void validateRestrictedConfigValues(
      String var, String userValue, String serverValue) throws HiveException {
    if ((userValue == null) != (serverValue == null)
        || (userValue != null && !userValue.equals(serverValue))) {
      String logValue = initConf.isHiddenConfig(var) ? "(hidden)" : serverValue;
      throw new HiveException(var + " is restricted from being set; server is configured"
          + " to use " + logValue + ", but the query configuration specifies " + userValue);
    }
  }
}