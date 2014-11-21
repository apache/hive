/**
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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/**
 * Helper class that can be used by authorization implementations to set a
 * default list of 'safe' HiveConf parameters that can be edited by user. It
 * uses HiveConf white list parameters to enforce this. This can be called from
 * HiveAuthorizer.applyAuthorizationConfigPolicy
 *
 * The set of config parameters that can be set is restricted to parameters that
 * don't allow for any code injection, and config parameters that are not
 * considered an 'admin config' option.
 *
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
@Unstable
public class SettableConfigUpdater {

  public static void setHiveConfWhiteList(HiveConf hiveConf) throws HiveAuthzPluginException {

    String whiteListParamsStr = hiveConf
        .getVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST);

    if(whiteListParamsStr == null || whiteListParamsStr.trim().isEmpty()) {
      throw new HiveAuthzPluginException("Configuration parameter "
          + ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST.varname
          + " is not iniatialized.");
    }

    // append regexes that user wanted to add
    String whiteListAppend = hiveConf
        .getVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND);
    if (whiteListAppend != null && !whiteListAppend.trim().equals("")) {
      whiteListParamsStr = whiteListParamsStr + "|" + whiteListAppend;
    }

    hiveConf.setModifiableWhiteListRegex(whiteListParamsStr);

    // disallow udfs that can potentially allow untrusted code execution
    // if admin has already customized this list, honor that
    String curBlackList = hiveConf.getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST);
    if (curBlackList == null || curBlackList.trim().isEmpty()) {
      hiveConf.setVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST, "reflect,reflect2,java_method");
    }
  }

}
