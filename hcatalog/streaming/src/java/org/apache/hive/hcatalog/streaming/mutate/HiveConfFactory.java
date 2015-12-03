/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates/configures {@link HiveConf} instances with required ACID attributes. */
public class HiveConfFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HiveConfFactory.class);
  private static final String TRANSACTION_MANAGER = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  public static HiveConf newInstance(Configuration configuration, Class<?> clazz, String metaStoreUri) {
    HiveConf hiveConf = null;
    if (configuration != null) {
      if (!HiveConf.class.isAssignableFrom(configuration.getClass())) {
        hiveConf = new HiveConf(configuration, clazz);
      } else {
        hiveConf = (HiveConf) configuration;
      }
    }

    if (hiveConf == null) {
      hiveConf = HiveConfFactory.newInstance(clazz, metaStoreUri);
    } else {
      HiveConfFactory.overrideSettings(hiveConf);
    }
    return hiveConf;
  }

  public static HiveConf newInstance(Class<?> clazz, String metaStoreUri) {
    HiveConf conf = new HiveConf(clazz);
    if (metaStoreUri != null) {
      setHiveConf(conf, HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    overrideSettings(conf);
    return conf;
  }

  public static void overrideSettings(HiveConf conf) {
    setHiveConf(conf, HiveConf.ConfVars.HIVE_TXN_MANAGER, TRANSACTION_MANAGER);
    setHiveConf(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    setHiveConf(conf, HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    // Avoids creating Tez Client sessions internally as it takes much longer currently
    setHiveConf(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, String value) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Overriding HiveConf setting : {} = {}", var, value);
    }
    conf.setVar(var, value);
  }

  private static void setHiveConf(HiveConf conf, HiveConf.ConfVars var, boolean value) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Overriding HiveConf setting : {} = {}", var, value);
    }
    conf.setBoolVar(var, value);
  }

}
