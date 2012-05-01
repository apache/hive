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
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.thrift.TException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An abstract class which should be extended by hooks which read configurations from an SMC
 * config tier.
 */
public abstract class AbstractSmcConfigHook {

  static final private Log LOG = LogFactory.getLog(AbstractSmcConfigHook.class);

  private static final String CONFIG_FIELD = "config";
  private static final String ENABLED_FIELD = "enabled";
  private ThreadLocal<ConnectionUrlFactory> urlFactory = null;

  /**
   * Given a HiveConf, checks if the SMC hook enabled config is set to true
   *
   * @param conf
   * @return
   */
  protected boolean isEnabled(HiveConf conf) {
    boolean enabled = conf.getBoolean(FBHiveConf.ENABLED_CONFIG, false);

    if (!enabled) {
      LOG.error("SMC hook is not enabled.");
    }

    return enabled;
  }

  /**
   * In each top level config object (jo) there is an enabled field.  This method checks that that
   * field exists, is set properly, and is set to true.
   *
   * @param jo
   * @param packageName
   * @return
   * @throws JSONException
   */
  protected boolean isConfigEnabled(JSONObject jo, String packageName) throws JSONException {
    boolean enabled = false;

    Object enabledObj = null;

    if (jo.has(ENABLED_FIELD)) {
      enabledObj = jo.get(ENABLED_FIELD);
    }

    if (enabledObj == null || !(enabledObj instanceof Boolean) ) {
      LOG.error("enabled not properly set!");
      return false;
    }

    enabled = enabledObj.equals(Boolean.TRUE);

    if (!enabled) {
      LOG.error("package " + packageName + " is not enabled");
    }

    return enabled;
  }

  /**
   * Given a HiveConf object, this method goes to the config tier and retrieves the underlying
   * config object (whether that's an array, object, or any other type of JSON).  It also performs
   * checks that the tier can be retrieved, the package name is set, the config is enabled, etc.
   *
   * @param conf
   * @return
   * @throws JSONException
   * @throws ServiceException
   * @throws TException
   */
  protected Object getConfigObject(HiveConf conf)
      throws JSONException, Exception, TException {

    // Get the properties for this package
    String packageName = conf.get(FBHiveConf.FB_CURRENT_CLUSTER);
    if (packageName == null) {
      LOG.error("Unable to use configs stored in SMC - no hive package set.");
      return null;
    }

    if (urlFactory == null) {
      urlFactory = new ThreadLocal<ConnectionUrlFactory>();
      urlFactory.set(HookUtils.getUrlFactory(conf,
                                             FBHiveConf.CONNECTION_FACTORY, null, null, null));
    }

    String s = urlFactory.get().getValue(conf.get(FBHiveConf.HIVE_CONFIG_TIER), packageName);
    JSONObject jo = new JSONObject(s);

    Object configObj = null;

    if (!isConfigEnabled(jo, packageName)) {
      return null;
    }

    if (jo.has(CONFIG_FIELD)) {
      configObj = jo.get(CONFIG_FIELD);
    }

    return configObj;
  }
}
