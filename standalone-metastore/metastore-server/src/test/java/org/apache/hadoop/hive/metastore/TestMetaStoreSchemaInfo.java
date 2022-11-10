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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test MetaStoreSchemaInfo
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStoreSchemaInfo {

  @Test
  public void testIsVersionCompatible() throws Exception {
    // first argument is hiveVersion, it is compatible if 2nd argument - dbVersion is
    // greater than or equal to it
    // check the compatible case
    IMetaStoreSchemaInfo metastoreSchemaInfo =
        MetaStoreSchemaInfoFactory.get(MetastoreConf.newMetastoreConf());
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.0.1", "0.0.1"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.0.1", "0.0.2"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("1.0.2", "2.0.1"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.0.9", "9.0.0"));

    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-alpha-2"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-alpha"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.1"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-beta"));

    // check equivalent versions, should be compatible
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.13.0", "0.13.1"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.13.1", "0.13.0"));

    // check incompatible versions
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("0.1.1", "0.1.0"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.1", "0.1.0"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.1", "4.0.0-alpha-1"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.0", "4.0.0-alpha-1"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha-2", "4.0.0-alpha-1"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.0-alpha", "4.0.0-alpha-1"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.0-beta", "4.0.0-alpha-1"));
  }

}
