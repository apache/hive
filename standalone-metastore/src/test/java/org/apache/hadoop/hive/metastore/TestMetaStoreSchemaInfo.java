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
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("3.1.3000.2021.0.0-b8", "3.1.3000.7.2.8.0"));

    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("3.1.3001.2021.0.0-b8", "3.1.3000.7.2.8.0"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("3.1.3000.2021.0.0-h10-b8", "3.1.3000.7.2.8.0"));


    // check equivalent versions, should be compatible
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.13.0", "0.13.1"));
    Assert.assertTrue(metastoreSchemaInfo.isVersionCompatible("0.13.1", "0.13.0"));

    // check incompatible versions
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("0.1.1", "0.1.0"));
    Assert.assertFalse(metastoreSchemaInfo.isVersionCompatible("4.0.1", "0.1.0"));

  }

  @Test
  public void testGetUnifiedVersion() throws Exception {
    Assert.assertEquals(
        CDHMetaStoreSchemaInfo.CDHVersion.convertToUnifiedVersionString("3.1.3000.2021.0.0-b8"),
        "3.1.3000.2021.0.0.0-b8");
    Assert.assertEquals(
        CDHMetaStoreSchemaInfo.CDHVersion.convertToUnifiedVersionString("3.1.3000.2021.0.0-h9-b8"),
        "3.1.3000.2021.0.0.9-b8");
    Assert.assertEquals(
        CDHMetaStoreSchemaInfo.CDHVersion.convertToUnifiedVersionString("3.1.3000.2021.0.0-h9"),
        "3.1.3000.2021.0.0.9");
    Assert.assertEquals(
        CDHMetaStoreSchemaInfo.CDHVersion.convertToUnifiedVersionString("3.1.3000.2021.0.0"),
        "3.1.3000.2021.0.0.0");
    CDHMetaStoreSchemaInfo.CDHVersion ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.3000.2021.0.0-h9-b8");
    Assert.assertEquals(ver1.getCdhVersionString(), "2021.0.0.9");
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.3000.2021.0.0-b8");
    Assert.assertEquals(ver1.getCdhVersionString(), "2021.0.0.0");
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.3000.7.2.9.0-79");
    Assert.assertEquals(ver1.getCdhVersionString(), "7.2.9.0");
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.1.7.2.10.0-SNAPSHOT");
    Assert.assertEquals(ver1.getCdhVersionString(), "7.2.10.0");
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.1.7.2.10.0");
    Assert.assertEquals(ver1.getCdhVersionString(), "7.2.10.0");
    //Following two cases assume snapshot was removed
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.3000.2021.0.0");
    Assert.assertEquals(ver1.getCdhVersionString(), "2021.0.0.0");
    ver1 = new CDHMetaStoreSchemaInfo.CDHVersion("3.1.3000.2021.0.0-h10");
    Assert.assertEquals(ver1.getCdhVersionString(), "2021.0.0.10");
  }

}
