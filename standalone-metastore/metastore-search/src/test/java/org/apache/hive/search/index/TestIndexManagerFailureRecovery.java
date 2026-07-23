/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.config.IndexStoreOptions;
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.metastore.MetastoreIndexSchema;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestIndexManagerFailureRecovery {

  @Test
  public void unhealthyStatusBlocksSearchUntilRecovered() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(IndexStoreOptions.MEMORY, true);
    conf.set(IndexOptions.INDEX_NAME, "test_index");
    conf.set(InferenceOptions.EMBEDDER_NAME, "stub-model");
    IndexMapping mapping = MetastoreIndexSchema.defaultHiveTablesMapping("test_index", "stub-model", conf);

    try (IndexManager indexManager = IndexManager.open(mapping, conf)) {
      IndexNotHealthyException failure =
          new IndexNotHealthyException("notification apply failed");
      indexManager.notifyIndexState(false, failure);
      assertThrows(IndexNotHealthyException.class, indexManager::checkIndexState);

      indexManager.notifyIndexState(true);
      indexManager.checkIndexState();
    }
  }
}
