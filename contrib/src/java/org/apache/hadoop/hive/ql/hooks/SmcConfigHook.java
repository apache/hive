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

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.json.JSONObject;
/**
 * Retrieves and sets Hive config key/values based on a config stored in the
 * properties of an SMC tier. This is useful for quick changes to the config
 * that should apply to a particular package of Hive. (e.g. silver.trunk). The
 * advantage over a XML file edit is that it's persistent between hotfixes and
 * we have a history of what changes were made. But since this is a hook that
 * runs after query compilation, it is limited in what values it can effectively
 * change.
 *
 * The configs are supposed to be stored in the properties of an SMC tier. The
 * name of the property corresponds to the hive package. The value of the
 * property is a JSON object that holds 1) an enabled field that controls
 * whether the key-value pairs should be applied 2) a config field that holds
 * the actual key-value pairs.
 *
 *    (Property)hivePackageName -> {enabled : boolean,
 *                                  configs : {key1 : value1,
 *                                             key2 : value2..
 *                                            }
 *                                 }
 *
 * The primary application of this hook is to modify the behavior of the
 * jobtracker hook. For the configs to apply to the hook, it must be listed
 * before the jobtracker hook in hive.exec.pre.hooks
 */
public class SmcConfigHook extends AbstractSmcConfigHook implements ExecuteWithHookContext {

  static final private Log LOG = LogFactory.getLog(SmcConfigHook.class);

  @Override
  public void run(HookContext hookContext) throws Exception {
    HiveConf conf = hookContext.getConf();

    if (!isEnabled(conf)) {
      return;
    }

    Object configObj = getConfigObject(conf);

    if (configObj == null || !(configObj instanceof JSONObject) ) {
      LOG.error("config not properly set!");
      return;
    }

    // Sanity checks pass, apply all the configs.
    JSONObject configJson = (JSONObject) configObj;
    @SuppressWarnings("unchecked")
    Iterator<String> i = (Iterator<String>) configJson.keys();
    while(i.hasNext()) {
      String key = i.next();
      Object valueObj = configJson.get(key);
      String value = valueObj.toString();

      conf.set(key, value);
      LOG.debug("Setting " + key + " to " + value);
    }
  }

}
