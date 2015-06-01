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

package org.apache.hadoop.hive;

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

    conf.setVar(ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:;databaseName=" + newDbLoc
        + ";create=true");
  }

}
