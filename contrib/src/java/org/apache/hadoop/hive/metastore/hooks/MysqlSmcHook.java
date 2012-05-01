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

package org.apache.hadoop.hive.metastore.hooks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ConnectionUrlFactory;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;

public class MysqlSmcHook implements JDOConnectionURLHook {

  static final private Log LOG =
    LogFactory.getLog("hive.metastore.hooks.MysqlSmcHook");

  ConnectionUrlFactory urlFactory = null;

  @Override
  public String getJdoConnectionUrl(Configuration conf)
  throws Exception {

    String smcUrl = conf.get(FBHiveConf.METASTORE_SMC_URL);
    if (smcUrl == null) {
      throw new Exception(FBHiveConf.METASTORE_SMC_URL + " is not defined");
    }
    String mysqlTier = conf.get(FBHiveConf.METASTORE_MYSQL_TIER_VAR_NAME);
    if (mysqlTier == null) {
      throw new Exception(FBHiveConf.METASTORE_MYSQL_TIER_VAR_NAME + " is not defined");
    }
    String mysqlProps = conf.get(FBHiveConf.METASTORE_MYSQL_PROPS);
    if (mysqlProps == null) {
      throw new Exception(FBHiveConf.METASTORE_MYSQL_PROPS + " is not defined");
    }
    if (urlFactory == null) {
      urlFactory = HookUtils.getUrlFactory(
        conf,
        FBHiveConf.CONNECTION_FACTORY,
        null,
        FBHiveConf.METASTORE_MYSQL_TIER_VAR_NAME,
        null,
        FBHiveConf.METASTORE_MYSQL_PROPS);
    }

    urlFactory.updateProps(smcUrl, mysqlTier, mysqlProps);
    return urlFactory.getUrl();
  }

  @Override
  public void notifyBadConnectionUrl(String url) {
    LOG.error("Notified of a bad URL: " + url);
  }

}
