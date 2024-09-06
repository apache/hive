/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.parse.QueryTables;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestHiveTableScan {

  @Test
  public void testGetConventionWithWithHepPlannerCluster() {
    RelNode scan = createScanWithPlanner(new HepPlanner(HepProgram.builder().build()));
    assertNull(scan.getConvention());
  }

  @Test
  public void testGetConventionWithVolcanoPlannerCluster() {
    RelNode scan = createScanWithPlanner(new VolcanoPlanner());
    assertEquals(HiveRelNode.CONVENTION, scan.getConvention());
  }

  private static RelNode createScanWithPlanner(RelOptPlanner planner) {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    return new HiveTableScan(cluster, cluster.traitSet(), dummyRelOptTable(typeFactory), "alias", "alias", false,
        false);
  }

  private static RelOptHiveTable dummyRelOptTable(RelDataTypeFactory factory) {
    RelDataType tblType = factory.builder().add("col1", SqlTypeName.INTEGER).add("col2", SqlTypeName.VARCHAR).build();
    Table tblMeta = new Table("default", "dummy");
    return new RelOptHiveTable(null, factory, Arrays.asList("default", "dummy"), tblType, tblMeta,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), new HiveConf(),
        new QueryTables(), new HashMap<>(), new HashMap<>(), new AtomicInteger(0));
  }
}
