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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Retrieves and sets Hive config key/values based on a config stored in the
 * properties of an SMC tier. This is useful for quick changes to the config
 * that should apply to a particular package of Hive. (e.g. silver.trunk). The
 * advantage over a XML file edit is that it's persistent between hotfixes and
 * we have a history of what changes were made. But since this is a hook that
 * runs at the very beginning of the Driver.run method, before compilation,
 * it should be able to effectively change most values that affect query
 * processing and execution.
 *
 * The configs are supposed to be stored in the properties of an SMC tier. The
 * name of the property corresponds to the hive package. The value of the
 * property is a JSON object that holds 1) an enabled field that controls
 * whether the key-value pairs should be applied 2) a config field that holds
 * an array of Objects
 *
 *    (Property)hivePackageName -> {enabled : boolean,
 *                                  configs : [
 *                                                {key : key1,
 *                                                 value : value1,
 *                                                 percentage : 50,
 *                                                 enforce : true
 *                                                },
 *                                                {key : key2,
 *                                                 value : value2
 *                                                }, ...
 *                                            ]
 *                                 }
 *
 * The key is the config variables key, value is the config variables value,
 * percentage is optional, if set, the change will only be applied to
 * approximately that percentage of queries, and enforce is also optional, if
 * true, even if the user explicitely set this config variable, it will be
 * overwritten.
 *
 * The primary application of this hook is to modify the behavior of Hive clients dynamically,
 * without a push, and for incremental rollouts of config changes.  E.g. if a feature is broken and
 * can be turned off using a config variable, this hook can be used to turn it off without rolling
 * back the push.  Also, if there is a change and we are not sure how it will perform at scale and
 * it can be controlled via a config, we can turn it on for increasing percentages of users using
 * this hook.
 */
public class SmcConfigDriverRunHook extends AbstractSmcConfigHook implements HiveDriverRunHook {

  static final private Log LOG = LogFactory.getLog(SmcConfigDriverRunHook.class);
  static final private String KEY_FIELD = "key";
  static final private String VALUE_FIELD = "value";
  static final private String PERCENTAGE_FIELD = "percentage";
  static final private String ENFORCE_FIELD = "enforce";

  @Override
  public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
    HiveConf conf = (HiveConf) hookContext.getConf();
    if (!isEnabled(conf)) {
      return;
    }

    Object configObj = getConfigObject(conf);

    if (configObj == null || !(configObj instanceof JSONArray) ) {
      LOG.error("config not properly set!");
      return;
    }

    // Sanity checks pass, apply all the configs.
    JSONArray configEntries = (JSONArray) configObj;
    for (int i = 0; i < configEntries.length(); i++) {
      JSONObject configEntry = configEntries.getJSONObject(i);
      Object percentage = new Integer(100);
      Object enforce = new Boolean(false);

      // Get the config key and value
      String key = configEntry.getString(KEY_FIELD);
      String value = configEntry.get(VALUE_FIELD).toString();

      LOG.debug("SmcConfigHook found configuration KEY: " + key + " VALUE: " + value);

      // If enforce is set to true, even if the user has set the value of this config variable
      // explicitely, we will overwrite it
      if (configEntry.has(ENFORCE_FIELD)) {
        enforce = configEntry.get(ENFORCE_FIELD);
      }

      LOG.debug("Enforce for key " + key + " is " + enforce.toString());

      if (!(enforce instanceof Boolean)) {
        LOG.error("enforce is not properly set for " + key);
        continue;
      }

      if (!(Boolean)enforce && SessionState.get() != null &&
          SessionState.get().getOverriddenConfigurations().containsKey(key)) {
        continue;
      }

      // If the percentage field is set to some number n, the configuration change will be made
      // to approximately n% of queries
      if (configEntry.has(PERCENTAGE_FIELD)) {
        percentage = configEntry.getInt(PERCENTAGE_FIELD);
      }

      LOG.debug("Percentage for key " + key + " is " + percentage.toString());

      if (!(percentage instanceof Integer)) {
        LOG.error("percentage is not properly set for " + key);
        continue;
      }

      if ((Integer)percentage != 100) {
        boolean diceRoll = false;

        try {
          diceRoll = HookUtils.rollDice(((Integer)percentage).intValue()/100f);
        } catch (Exception e) {
          LOG.error("percentage is not properly set for " + key);
          LOG.error(e.getMessage());
        }

        if (!diceRoll) {
          continue;
        }
      }

      conf.set(key, value);
    }
  }

  @Override
  public void postDriverRun(HiveDriverRunHookContext hookContext)
      throws Exception {
    // Do nothing
  }

}
