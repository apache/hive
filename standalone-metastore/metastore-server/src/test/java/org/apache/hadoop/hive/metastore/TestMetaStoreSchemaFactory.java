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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestMetaStoreSchemaFactory {
  private Configuration conf;

  @Before
  public void setup() {
    conf = MetastoreConf.newMetastoreConf();
  }

  @Test
  public void testDefaultConfig() {
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
  }

  @Test
  public void testWithConfigSet() {
    MetastoreConf.setVar(conf, ConfVars.SCHEMA_INFO_CLASS,
        MetaStoreSchemaInfo.class.getCanonicalName());
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance type of the class MetaStoreSchemaInfo",
        metastoreSchemaInfo instanceof MetaStoreSchemaInfo);
  }

  @Test
  public void testConstructor() {
    String className = MetastoreConf.getVar(conf, ConfVars.SCHEMA_INFO_CLASS,
        MetaStoreSchemaInfo.class.getCanonicalName());
    Class<?> clasz = null;
    try {
      clasz = conf.getClassByName(className);
      clasz.getConstructor(String.class, String.class);
    } catch (NoSuchMethodException | IllegalArgumentException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidClassName() {
    MetastoreConf.setVar(conf, ConfVars.SCHEMA_INFO_CLASS, "invalid.class.name");
    MetaStoreSchemaInfoFactory.get(conf);
  }
}
