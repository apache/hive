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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

import com.google.common.collect.ImmutableList;

public class HiveRelMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {

  public static final RelMetadataProvider SOURCE =
          ChainedRelMetadataProvider.of(
                  ImmutableList.of(
                          ReflectiveRelMetadataProvider.reflectiveSource(
                              BuiltInMethod.DISTRIBUTION.method, new HiveRelMdDistribution())));
  
  //~ Constructors -----------------------------------------------------------

  private HiveRelMdDistribution() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.Distribution> getDef() {
    return BuiltInMetadata.Distribution.DEF;
  }

  public RelDistribution distribution(HiveAggregate aggregate, RelMetadataQuery mq) {
    return new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
            aggregate.getGroupSet().asList());
  }

  public RelDistribution distribution(HiveJoin join, RelMetadataQuery mq) {
    return join.getDistribution();
  }

}
