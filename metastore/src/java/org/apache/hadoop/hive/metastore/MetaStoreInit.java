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

package org.apache.hadoop.hive.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hooks.JDOConnectionURLHook;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * MetaStoreInit defines functions to init/update MetaStore connection url.
 *
 */
public class MetaStoreInit {

  private static final Log LOG = LogFactory.getLog(MetaStoreInit.class);

  static class MetaStoreInitData {
    JDOConnectionURLHook urlHook = null;
    String urlHookClassName = "";
  }

  /**
   * Updates the connection URL in hiveConf using the hook
   *
   * @return true if a new connection URL was loaded into the thread local
   *         configuration
   */
  static boolean updateConnectionURL(HiveConf hiveConf, Configuration conf,
    String badUrl, MetaStoreInitData updateData)
      throws MetaException {
    String connectUrl = null;
    String currentUrl = MetaStoreInit.getConnectionURL(conf);
    try {
      // We always call init because the hook name in the configuration could
      // have changed.
      MetaStoreInit.initConnectionUrlHook(hiveConf, updateData);
      if (updateData.urlHook != null) {
        if (badUrl != null) {
          updateData.urlHook.notifyBadConnectionUrl(badUrl);
        }
        connectUrl = updateData.urlHook.getJdoConnectionUrl(hiveConf);
      }
    } catch (Exception e) {
      LOG.error("Exception while getting connection URL from the hook: " +
          e);
    }

    if (connectUrl != null && !connectUrl.equals(currentUrl)) {
      LOG.error(
          String.format("Overriding %s with %s",
              HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
              connectUrl));
      conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
          connectUrl);
      return true;
    }
    return false;
  }

  static String getConnectionURL(Configuration conf) {
    return conf.get(
        HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), "");
  }

  // Multiple threads could try to initialize at the same time.
  synchronized private static void initConnectionUrlHook(HiveConf hiveConf,
    MetaStoreInitData updateData) throws ClassNotFoundException {

    String className =
        hiveConf.get(HiveConf.ConfVars.METASTORECONNECTURLHOOK.toString(), "").trim();
    if (className.equals("")) {
      updateData.urlHookClassName = "";
      updateData.urlHook = null;
      return;
    }
    boolean urlHookChanged = !updateData.urlHookClassName.equals(className);
    if (updateData.urlHook == null || urlHookChanged) {
      updateData.urlHookClassName = className.trim();

      Class<?> urlHookClass = Class.forName(updateData.urlHookClassName, true,
          JavaUtils.getClassLoader());
      updateData.urlHook = (JDOConnectionURLHook) ReflectionUtils.newInstance(urlHookClass, null);
    }
    return;
  }
}
