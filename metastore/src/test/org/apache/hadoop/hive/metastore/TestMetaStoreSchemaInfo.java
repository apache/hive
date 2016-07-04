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

package org.apache.hadoop.hive.metastore;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test MetaStoreSchemaInfo
 */
public class TestMetaStoreSchemaInfo {

  @Test
  public void testIsVersionCompatible() throws Exception {
    // first argument is hiveVersion, it is compatible if 2nd argument - dbVersion is
    // greater than or equal to it
    // check the compatible case
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("0.0.1", "0.0.1"));
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("0.0.1", "0.0.2"));
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("1.0.2", "2.0.1"));
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("0.0.9", "9.0.0"));

    // check equivalent versions, should be compatible
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("0.13.0", "0.13.1"));
    Assert.assertTrue(MetaStoreSchemaInfo.isVersionCompatible("0.13.1", "0.13.0"));

    // check incompatible versions
    Assert.assertFalse(MetaStoreSchemaInfo.isVersionCompatible("0.1.1", "0.1.0"));
    Assert.assertFalse(MetaStoreSchemaInfo.isVersionCompatible("4.0.1", "0.1.0"));

  }

}
