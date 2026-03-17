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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import java.util.List;

public class HiveTableSpool extends TableSpool implements HiveRelNode {
  public HiveTableSpool(RelNode input, Type readType, Type writeType, RelOptTable table) {
    super(input.getCluster(), TraitsUtil.getDefaultTraitSet(input.getCluster()), input, readType, writeType, table);
  }

  public HiveTableSpool(RelInput relInput) {
    this(
        relInput.getInput(),
        Type.LAZY,
        Type.LAZY,
        RelOptTableImpl.create(null, relInput.getInput().getRowType(), (List<String>) relInput.get("table"), null));
  }

  @Override
  protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
    return new HiveTableSpool(input, readType, writeType, table);
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw) {
    return pw.input("input", getInput()).item("table", getTable().getQualifiedName());
  }
}
