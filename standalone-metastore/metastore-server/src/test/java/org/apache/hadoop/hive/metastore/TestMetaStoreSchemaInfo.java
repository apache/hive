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
    // first argument is hiveVersion, 2nd argument is dbVersion. Those are compactible

    // when major and minor version is the same, and db version is higer or equal
    Assert.assertTrue(SchemaInfo.isVersionCompatible("0.0.1", "0.0.1"));
    Assert.assertTrue(SchemaInfo.isVersionCompatible("0.0.1", "0.0.2"));

    // when major and minor version is the same, and db version is lower
    Assert.assertFalse(SchemaInfo.isVersionCompatible("2.1.9", "2.1.2"));

    // when ther are only incremental version differences and hive version has more or equal version parts
    Assert.assertTrue(SchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-alpha-2"));
    Assert.assertTrue(SchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-alpha"));
    Assert.assertTrue(SchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0"));
    Assert.assertTrue(SchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.1"));
    Assert.assertTrue(SchemaInfo.isVersionCompatible("4.0.0-alpha-1", "4.0.0-beta"));

    // check incompatible versions (minor or major version difference or db version has more version parts
    Assert.assertFalse(SchemaInfo.isVersionCompatible("4.0.1", "0.1.0"));
    Assert.assertFalse(SchemaInfo.isVersionCompatible("4.0.0", "4.1.0-alpha-1"));
    Assert.assertFalse(SchemaInfo.isVersionCompatible("4.0.0-alpha", "4.2.0-alpha-1"));
    Assert.assertFalse(SchemaInfo.isVersionCompatible("4.0.0-beta", "4.0.0-alpha-1"));
    Assert.assertFalse(SchemaInfo.isVersionCompatible("1.0.2", "2.0.1"));
    Assert.assertFalse(SchemaInfo.isVersionCompatible("2.0.2", "2.1.1"));
  }

}
