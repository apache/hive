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
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.mapper.PersistedRuntimeStats;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRuntimeStatsPersistence {

  GenericUDF udf = new GenericUDFConcat();

  CompilationOpContext cCtx = new CompilationOpContext();

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
  public void checkPersistJoinCondDesc() throws Exception {
    JoinCondDesc jcd = new JoinCondDesc(1, 2, 3);
    JoinCondDesc jcd2 = persistenceLoop(jcd, JoinCondDesc.class);
    assertEquals(jcd, jcd2);
  }

  OpTreeSignatureFactory signatureFactory = OpTreeSignatureFactory.newCache();

  @Test
  public void checkPersistingSigWorks() throws Exception {
    OpSignature sig = OpSignature.of(getTsOp(3));
    OpSignature sig2 = persistenceLoop(sig, OpSignature.class);
    assertEquals(sig, sig2);
  }

  @Test
  public void checkPersistingTreeSigWorks() throws Exception {
    OpTreeSignature sig = signatureFactory.getSignature(getFilTsOp(3, 4));
    OpTreeSignature sig2 = persistenceLoop(sig, OpTreeSignature.class);
    assertEquals(sig, sig2);
  }

  @Test
  public void checkCanStoreAsGraph() throws Exception {

    Operator<?> ts = getTsOp(0);
    Operator<?> fil1 = getFilterOp(1);
    Operator<?> fil2 = getFilterOp(2);
    Operator<?> fil3 = getFilterOp(3);

    connectOperators(ts, fil1);
    connectOperators(ts, fil2);
    connectOperators(fil1, fil3);
    connectOperators(fil2, fil3);

    OpTreeSignature sig = signatureFactory.getSignature(fil3);
    OpTreeSignature sig2 = persistenceLoop(sig, OpTreeSignature.class);

    assertEquals(sig, sig2);

    OpTreeSignature o0 = sig.getParentSig().get(0).getParentSig().get(0);
    OpTreeSignature o1 = sig.getParentSig().get(1).getParentSig().get(0);
    assertTrue("these have to be the same instance", o0 == o1);

    OpTreeSignature p0 = sig2.getParentSig().get(0).getParentSig().get(0);
    OpTreeSignature p1 = sig2.getParentSig().get(1).getParentSig().get(0);

    assertTrue("these have to be the same instance", p0 == p1);

    assertEquals(p0, p1);

  }

  // FIXME: redo test
  @Test
  public void checkCanStore() throws Exception {

    List<PersistedRuntimeStats> rsm = new ArrayList<>();
    rsm.add(new PersistedRuntimeStats(signatureFactory.getSignature(getTsOp(0)), new OperatorStats("ts0"), null));
    rsm.add(new PersistedRuntimeStats(signatureFactory.getSignature(getTsOp(1)), new OperatorStats("ts1"), null));

    List<PersistedRuntimeStats> rsm2 = persistenceLoop(rsm, List.class);
    OpTreeSignature k1 = rsm.iterator().next().sig;
    OpTreeSignature k2 = rsm2.iterator().next().sig;
    assertEquals(k1, k2);
    assertEquals(rsm, rsm2);
  }

  private <T> T persistenceLoop(T sig, Class<T> clazz) throws IOException {
    RuntimeStatsPersister sp = RuntimeStatsPersister.INSTANCE;
    String stored = sp.encode(sig);
    System.out.println(stored);
    T sig2 = sp.decode(stored, clazz);
    return sig2;
  }

  private Operator<? extends OperatorDesc> getFilterOp(int constVal) {
    ExprNodeDesc pred = new ExprNodeConstantDesc(constVal);
    FilterDesc fd = new FilterDesc(pred, true);
    Operator<? extends OperatorDesc> op = OperatorFactory.get(cCtx, fd);
    return op;
  }

  private Operator<TableScanDesc> getTsOp(int i) {
    Table tblMetadata = new Table("db", "table");
    TableScanDesc desc = new TableScanDesc("alias"/*+ cCtx.nextOperatorId()*/, tblMetadata);
    List<ExprNodeDesc> as =
        Lists.newArrayList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, Integer.valueOf(i)),
            new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "c1", "aa", false));
    ExprNodeGenericFuncDesc f1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, udf, as);
    desc.setFilterExpr(f1);
    Operator<TableScanDesc> ts = OperatorFactory.get(cCtx, desc);
    return ts;
  }

}
