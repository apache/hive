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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hooks.JDOConnectionURLHook;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * MetaStoreInit defines functions to init/update MetaStore connection url.
 *
 */
public class MetaStoreInit {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreInit.class);

  static class MetaStoreInitData {
    JDOConnectionURLHook urlHook = null;
    String urlHookClassName = "";
  }

  /**
   * Updates the connection URL in hiveConf using the hook (if a hook has been
   * set using hive.metastore.ds.connection.url.hook property)
   * @param originalConf - original configuration used to look up hook settings
   * @param activeConf - the configuration file in use for looking up db url
   * @param badUrl
   * @param updateData - hook information
   * @return true if a new connection URL was loaded into the thread local
   *         configuration
   * @throws MetaException
   */
  static boolean updateConnectionURL(Configuration originalConf, Configuration activeConf,
    String badUrl, MetaStoreInitData updateData)
      throws MetaException {
    String connectUrl = null;
    String currentUrl = MetaStoreInit.getConnectionURL(activeConf);
    try {
      // We always call init because the hook name in the configuration could
      // have changed.
      initConnectionUrlHook(originalConf, updateData);
      if (updateData.urlHook != null) {
        if (badUrl != null) {
          updateData.urlHook.notifyBadConnectionUrl(badUrl);
        }
        connectUrl = updateData.urlHook.getJdoConnectionUrl(originalConf);
      }
    } catch (Exception e) {
      LOG.error("Exception while getting connection URL from the hook: " +
          e);
    }

    if (connectUrl != null && !connectUrl.equals(currentUrl)) {
      LOG.error(
          String.format("Overriding %s with %s",
              MetastoreConf.ConfVars.CONNECT_URL_KEY.toString(),
              connectUrl));
      MetastoreConf.setVar(activeConf, ConfVars.CONNECT_URL_KEY, connectUrl);
      return true;
    }
    return false;
  }

  static String getConnectionURL(Configuration conf) {
    return MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY, "");
  }

  // Multiple threads could try to initialize at the same time.
  synchronized private static void initConnectionUrlHook(Configuration conf,
    MetaStoreInitData updateData) throws ClassNotFoundException {

    String className = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_HOOK, "").trim();
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
  }
}
