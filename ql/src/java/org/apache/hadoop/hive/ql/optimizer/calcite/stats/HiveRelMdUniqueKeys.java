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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveRelMdUniqueKeys implements MetadataHandler<BuiltInMetadata.UniqueKeys> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.UNIQUE_KEYS.method, new HiveRelMdUniqueKeys());

  @Override
  public MetadataDef<BuiltInMetadata.UniqueKeys> getDef() {
    return BuiltInMetadata.UniqueKeys.DEF;
  }


  public Set<ImmutableBitSet> getUniqueKeys(HiveTableScan rel, RelMetadataQuery mq,
                                            boolean ignoreNulls) {
    RelOptHiveTable tbl = (RelOptHiveTable) rel.getTable();
    List<ImmutableBitSet> keyList = tbl.getNonNullableKeys();
    if (keyList != null) {
      Set<ImmutableBitSet> keySet = new HashSet<>(keyList);
      return keySet;
    }
    return null;
  }
}
