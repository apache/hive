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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestMetaStoreUtils {

  @Test
  public void testcolumnsIncludedByNameType() {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col1a = new FieldSchema("col1", "string", "col1 but with a different comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1), Arrays.asList(col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1), Arrays.asList(col1a)));
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1, col2)));
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col2, col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1, col2, col3)));
    Assert.assertTrue(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col3, col2, col1)));
    Assert.assertFalse(MetaStoreUtils.columnsIncludedByNameType(Arrays.asList(col1, col2), Arrays.asList(col1)));
  }
}
