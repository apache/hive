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

package org.apache.hadoop.hive.ql.optimizer.signature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestOperatorSignature {

  GenericUDF udf = new GenericUDFConcat();

  CompilationOpContext cCtx = new CompilationOpContext();

  @Test
  public void testFilterOpEquals() {
    Operator<? extends OperatorDesc> op7 = getFilterOp(7);
    Operator<? extends OperatorDesc> op8 = getFilterOp(8);
    Operator<? extends OperatorDesc> op7b = getFilterOp(7);

    checkEquals(op7, op7b);
    checkNotEquals(op7, op8);
  }

  @Test
  public void testTree1() {
    Operator<?> tr37 = getFilTsOp(3, 7);
    Operator<?> tr37a = getFilTsOp(3, 7);
    Operator<?> tr17 = getFilTsOp(1, 7);
    Operator<?> tr31 = getFilTsOp(3, 1);

    checkEquals(tr37, tr37a);

    checkTreeNotEquals(tr37, tr17);
    checkTreeEquals(tr37, tr37a);
    checkTreeNotEquals(tr37, tr31);
  }

  private Operator<?> getFilTsOp(int i, int j) {
    Operator<TableScanDesc> ts = getTsOp(i);
    Operator<? extends OperatorDesc> fil = getFilterOp(j);

    connectOperators(ts, fil);

    return fil;
  }

  private void connectOperators(Operator<?> parent, Operator<?> child) {
    parent.getChildOperators().add(child);
    child.getParentOperators().add(parent);
  }

  @Test
  public void testTableScand() {
    Operator<TableScanDesc> t1 = getTsOp(3);
    Operator<TableScanDesc> t1a = getTsOp(3);
    Operator<TableScanDesc> t2 = getTsOp(4);

    checkEquals(t1, t1a);
    checkNotEquals(t1, t2);
  }

  public static void checkEquals(Operator<?> o1, Operator<?> o2) {
    assertTrue(o1.logicalEquals(o2));
    OpSignature s1 = OpSignature.of(o1);
    OpSignature s2 = OpSignature.of(o2);

    s1.proveEquals(s2);
    assertTrue("sigCmp", s1.signatureCompare(s2));
    assertEquals(s1.hashCode(), s2.hashCode());
    assertEquals(s1, s2);
  }


  public static void checkNotEquals(Operator<? extends OperatorDesc> o1, Operator<? extends OperatorDesc> o2) {
    assertFalse(o1.logicalEquals(o2));
    OpSignature s1 = OpSignature.of(o1);
    OpSignature s2 = OpSignature.of(o2);

    assertFalse(s1.signatureCompare(s2));
    // this might be a little bit too much...but in most cases this should be true
    assertNotEquals(s1.hashCode(), s2.hashCode());
    assertNotEquals(s1, s2);
  }

  public static void checkTreeEquals(Operator<?> o1, Operator<?> o2) {
    OpTreeSignature ts1 = OpTreeSignature.of(o1);
    OpTreeSignature ts2 = OpTreeSignature.of(o2);

    assertEquals(ts1.hashCode(), ts2.hashCode());
    assertEquals(ts1, ts2);
  }

  public static void checkTreeNotEquals(Operator<? extends OperatorDesc> o1, Operator<? extends OperatorDesc> o2) {

    OpTreeSignature ts1 = OpTreeSignature.of(o1);
    OpTreeSignature ts2 = OpTreeSignature.of(o2);

    assertNotEquals(ts1.hashCode(), ts2.hashCode());
    ts1.equals(ts2);
    assertNotEquals(ts1, ts2);
  }


  private Operator<? extends OperatorDesc> getFilterOp(int constVal) {
    ExprNodeDesc pred = new ExprNodeConstantDesc(constVal);
    FilterDesc fd = new FilterDesc(pred, true);
    Operator<? extends OperatorDesc> op = OperatorFactory.get(cCtx, fd);
    return op;
  }

  private Operator<TableScanDesc> getTsOp(int i) {
    Table tblMetadata = new Table("db", "table");
    TableScanDesc desc = new TableScanDesc("alias_" + cCtx.nextOperatorId(), tblMetadata);
    List<ExprNodeDesc> as =
        Lists.newArrayList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, Integer.valueOf(i)),
            new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "c1", "aa", false));
    ExprNodeGenericFuncDesc f1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, udf, as);
    desc.setFilterExpr(f1);
    Operator<TableScanDesc> ts = OperatorFactory.get(cCtx, desc);
    return ts;
  }



}
