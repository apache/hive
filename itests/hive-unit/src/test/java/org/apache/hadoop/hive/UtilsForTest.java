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

package org.apache.hadoop.hive;

import java.io.File;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/**
 * Test utilities
 */
public class UtilsForTest {
  /**
   * Use this if you want a fresh metastore for your test, without any existing entries.
   * It updates the configuration to point to new derby dir location
   * @param conf HiveConf to be updated
   * @param newloc new location within test temp dir for the metastore db
   */
  public static void setNewDerbyDbLocation(HiveConf conf, String newloc) {
    String newDbLoc = System.getProperty("test.tmp.dir") + newloc + "metastore_db";

    conf.setVar(ConfVars.METASTORE_CONNECT_URL_KEY, "jdbc:derby:;databaseName=" + newDbLoc
        + ";create=true");
  }

  /**
   * Do the variable expansion by calling "set" on each variable.
   * When MR jobs are run, under some circumstances they fail because
   * the variable expansion fails after changes in Hadoop to prevent
   * variable expansion for JobHistoryServer. So expanding them ahead
   * so that variables like {test.tmp.dir} get expanded.
   * @param hiveConf
   */
  public static void expandHiveConfParams(HiveConf hiveConf) {
    Iterator<Map.Entry<String, String>> iter = hiveConf.iterator();
    while (iter.hasNext()) {
      String key = iter.next().getKey();
      hiveConf.set(key, hiveConf.get(key));
    }
  }

  public static HiveConf getHiveOnTezConfFromDir(String confDir) throws Exception {
    HiveConf.setHiveSiteLocation(
        new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    HiveConf hiveConf = new HiveConf();
    hiveConf
        .addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    return hiveConf;
  }

}
