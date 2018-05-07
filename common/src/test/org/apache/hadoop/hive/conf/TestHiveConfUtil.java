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
package org.apache.hadoop.hive.conf;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * TestHiveConfUtil
 *
 */
public class TestHiveConfUtil {

  private HiveConf conf = new HiveConf();

  @Before
  public void init() {
    conf.setBoolean("dummyBoolean", true);
    conf.set("dummy", "aaa");
    conf.set("dummy2", "aaa");
    conf.set("3dummy", "aaa");
  }

  @Test
  public void testHideNonStringVar() throws Exception {
   Assert.assertTrue(conf.getBoolean("dummyBoolean", false));
   Assert.assertEquals("true", conf.get("dummyBoolean"));
   HiveConfUtil.stripConfigurations(conf, Sets.newHashSet("dummyBoolean"));
   Assert.assertFalse(conf.getBoolean("dummyBoolean", false));
   Assert.assertEquals("", conf.get("dummyBoolean"));
  }

  @Test
  public void testHideStringVar() throws Exception {
    Assert.assertEquals("aaa", conf.get("dummy"));
    HiveConfUtil.stripConfigurations(conf, Sets.newHashSet("dummy"));
    Assert.assertEquals("", conf.get("dummy"));
  }

  @Test
  public void testHideMultipleVars() throws Exception {
    Assert.assertEquals("aaa", conf.get("dummy"));
    Assert.assertEquals("aaa", conf.get("dummy2"));
    Assert.assertEquals("aaa", conf.get("3dummy"));
    HiveConfUtil.stripConfigurations(conf, Sets.newHashSet("dummy"));
    Assert.assertEquals("", conf.get("dummy"));
    Assert.assertEquals("", conf.get("dummy2"));
    Assert.assertEquals("aaa", conf.get("3dummy"));
  }

}
