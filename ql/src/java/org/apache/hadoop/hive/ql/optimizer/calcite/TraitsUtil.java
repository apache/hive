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

package org.apache.hadoop.hive.ql.optimizer.calcite;


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;

public class TraitsUtil {
  public static RelTraitSet getSortTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelCollation collation) {
    return traitSet.plus(collation);
  }

  public static RelTraitSet getDefaultTraitSet(RelOptCluster cluster) {
    return cluster.traitSetOf(HiveRelNode.CONVENTION, RelCollationImpl.EMPTY);
  }
}
