/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

@Category(MetastoreUnitTest.class)
public class TestParseDbName {

  @Test
  public void testParseDbNameEdgeCases() throws MetaException {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "hive");

    // Reviewer edge cases:
    // @ - Desigates @ as database in default catalog
    String[] result = MetaStoreUtils.parseDbName("@", conf);
    Assert.assertArrayEquals(new String[]{"hive", "@"}, result);

    // @! -> ["", ""]
    result = MetaStoreUtils.parseDbName("@!", conf);
    Assert.assertArrayEquals(new String[]{"", ""}, result);

    // @# -> ["", null]
    result = MetaStoreUtils.parseDbName("@#", conf);
    Assert.assertArrayEquals(new String[]{"", null}, result);

    // @#db1 -> ["", "db1"]
    result = MetaStoreUtils.parseDbName("@#db1", conf);
    Assert.assertArrayEquals(new String[]{"", "db1"}, result);

    // @cat1 - Desigates @cat1 as database in default catalog
    result = MetaStoreUtils.parseDbName("@cat1", conf);
    Assert.assertArrayEquals(new String[]{"hive", "@cat1"}, result);

    // @cat1# -> ["cat1", null]
    result = MetaStoreUtils.parseDbName("@cat1#", conf);
    Assert.assertArrayEquals(new String[]{"cat1", null}, result);

    // @cat1#! -> ["cat1", ""]
    result = MetaStoreUtils.parseDbName("@cat1#!", conf);
    Assert.assertArrayEquals(new String[]{"cat1", ""}, result);

    // @cat1#@db1 -> ["cat1", "@db1"]
    result = MetaStoreUtils.parseDbName("@cat1#@db1", conf);
    Assert.assertArrayEquals(new String[]{"cat1", "@db1"}, result);

    // @cat1##db1 -> ["cat1", "#db1"]
    result = MetaStoreUtils.parseDbName("@cat1##db1", conf);
    Assert.assertArrayEquals(new String[]{"cat1", "#db1"}, result);

    // @cat1#db1 -> ["cat1", "db1"]
    result = MetaStoreUtils.parseDbName("@cat1#db1", conf);
    Assert.assertArrayEquals(new String[]{"cat1", "db1"}, result);

    // @cat1#db1! -> ["cat1#db1", ""]
    result = MetaStoreUtils.parseDbName("@cat1#db1!", conf);
    Assert.assertArrayEquals(new String[]{"cat1#db1", ""}, result);

    // @cat1! -> ["cat1", ""]
    result = MetaStoreUtils.parseDbName("@cat1!", conf);
    Assert.assertArrayEquals(new String[]{"cat1", ""}, result);

    // #db1 -> ["hive", "#db1"]
    result = MetaStoreUtils.parseDbName("#db1", conf);
    Assert.assertArrayEquals(new String[]{"hive", "#db1"}, result);

    // #! -> ["hive", "#!"]
    result = MetaStoreUtils.parseDbName("#!", conf);
    Assert.assertArrayEquals(new String[]{"hive", "#!"}, result);

    // # -> ["hive", "#"]
    result = MetaStoreUtils.parseDbName("#", conf);
    Assert.assertArrayEquals(new String[]{"hive", "#"}, result);
  }

  private void assertThrowsMetaException(String dbName, Configuration conf) {
    try {
      String[] result = MetaStoreUtils.parseDbName(dbName, conf);
      Assert.fail("Expected MetaException for " + dbName + ", but got " + Arrays.toString(result));
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("is prepended with the catalog marker but does not appear to have a catalog name in it"));
    }
  }
}
