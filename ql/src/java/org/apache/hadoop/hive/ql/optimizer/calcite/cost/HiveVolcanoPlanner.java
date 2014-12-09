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

package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;

/**
 * Refinement of {@link org.apache.calcite.plan.volcano.VolcanoPlanner} for Hive.
 * 
 * <p>
 * It uses {@link org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost} as
 * its cost model.
 */
public class HiveVolcanoPlanner extends VolcanoPlanner {
  private static final boolean ENABLE_COLLATION_TRAIT = true;

  /** Creates a HiveVolcanoPlanner. */
  public HiveVolcanoPlanner() {
    super(HiveCost.FACTORY, null);
  }

  public static RelOptPlanner createPlanner() {
    final VolcanoPlanner planner = new HiveVolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    if (ENABLE_COLLATION_TRAIT) {
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    }
    return planner;
  }
}
