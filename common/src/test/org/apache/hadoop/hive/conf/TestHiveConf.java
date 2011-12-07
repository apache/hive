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
package org.apache.hadoop.hive.conf;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;


/**
 * TestHiveConf
 *
 */
public class TestHiveConf extends TestCase {

  public void testHiveSitePath() throws Exception {
    String expectedPath = System.getProperty("test.build.resources") + "/hive-site.xml";
    assertEquals(expectedPath, new HiveConf().getHiveSitePath());
  }

  public void testHiveSiteProperties() throws Exception {
    HiveConf conf = new HiveConf();

    // ConfVar only defined in HiveConf
    assertEquals(ConfVars.HIVESKEWJOINKEY.defaultIntVal, conf.getIntVar(ConfVars.HIVESKEWJOINKEY));

    // ConfVar overridden in local hive-site.xml
    assertEquals("hive-site.xml", conf.get("javax.jdo.option.ConnectionDriverName"));

    // Hadoop property overridden in ConfVars and hive-site.xml
    assertEquals("hive-site.xml", conf.get("mapred.reduce.tasks"));

    // Test property defined in hive-site.xml only
    assertEquals("hive-site.xml", conf.get("test.property1"));

    // Test Hive conf property variable substitution in hive-site.xml
    assertEquals(conf.get("hive.exec.default.partition.name"), conf.get("test.var.hiveconf.property"));
  }
}
