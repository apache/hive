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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Round trips real operator trees (filter with UDF expressions, reduce sink, select, file sink) through both plan
 * serialization engines and compares the resulting operator graphs.
 */
public class TestPlanSerializationRealPlans {
  private static final CompilationOpContext CTX = new CompilationOpContext();

  @BeforeClass
  public static void init() throws Exception {
    // Registers the built-in UDFs used by the expressions below.
    FunctionRegistry.getFunctionInfo("<");
  }

  private static ExprNodeColumnDesc column(String name) {
    return new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, name, "", false);
  }

  private static FilterDesc filterDesc(String column) throws Exception {
    List<ExprNodeDesc> c1 = new ArrayList<>();
    c1.add(column(column));
    ExprNodeDesc lhs = new ExprNodeGenericFuncDesc(TypeInfoFactory.doubleTypeInfo,
        FunctionRegistry.getFunctionInfo(serdeConstants.DOUBLE_TYPE_NAME).getGenericUDF(), c1);
    List<ExprNodeDesc> c2 = new ArrayList<>();
    c2.add(new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 100L));
    ExprNodeDesc rhs = new ExprNodeGenericFuncDesc(TypeInfoFactory.doubleTypeInfo,
        FunctionRegistry.getFunctionInfo(serdeConstants.DOUBLE_TYPE_NAME).getGenericUDF(), c2);
    List<ExprNodeDesc> c3 = new ArrayList<>();
    c3.add(lhs);
    c3.add(rhs);
    ExprNodeDesc cmp = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getFunctionInfo("<").getGenericUDF(), c3);
    return new FilterDesc(cmp, false);
  }

  /** map: RS; reduce: SEL -> FS. Mirrors TestExecDriver.populateMapRedPlan1. */
  private static MapredWork mapRedPlan() throws SemanticException, HiveException {
    MapredWork mr = PlanUtils.getMapRedWork();
    List<String> outputColumns = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      outputColumns.add("_col" + i);
    }
    Operator<ReduceSinkDesc> rs = OperatorFactory.get(CTX, PlanUtils.getReduceSinkDesc(
        Utilities.makeList(column("key")), Utilities.makeList(column("value")), outputColumns, true, -1, 1, -1,
        AcidUtils.Operation.NOT_ACID, NullOrdering.NULLS_LAST));
    mr.getMapWork().addMapWork(new Path("/warehouse/src"), "a", rs,
        new PartitionDesc(Utilities.defaultTd, null));

    ReduceWork rWork = new ReduceWork();
    rWork.setNumReduceTasks(1);
    rWork.setKeyDesc(rs.getConf().getKeySerializeInfo());
    rWork.getTagToValueDesc().add(rs.getConf().getValueSerializeInfo());
    mr.setReduceWork(rWork);

    Operator<FileSinkDesc> fs = OperatorFactory.get(CTX, new FileSinkDesc(new Path("/tmp/out"),
        Utilities.defaultTd, false));
    List<ExprNodeDesc> cols = new ArrayList<>();
    cols.add(column(Utilities.ReduceField.VALUE + "." + outputColumns.get(1)));
    List<String> names = new ArrayList<>();
    names.add(HiveConf.getColumnInternalName(2));
    Operator<SelectDesc> sel = OperatorFactory.get(new SelectDesc(cols, names), fs);
    rWork.setReducer(sel);
    return mr;
  }

  /** map: FIL -> FS with UDF expression tree. Mirrors TestExecDriver.populateMapPlan1. */
  private static MapredWork mapOnlyPlan() throws Exception {
    MapredWork mr = PlanUtils.getMapRedWork();
    Operator<FileSinkDesc> fs = OperatorFactory.get(CTX, new FileSinkDesc(new Path("/tmp/out"),
        Utilities.defaultTd, true));
    Operator<FilterDesc> fil = OperatorFactory.get(filterDesc("key"), fs);
    mr.getMapWork().addMapWork(new Path("/warehouse/src"), "a", fil,
        new PartitionDesc(Utilities.defaultTd, null));
    return mr;
  }

  private static HiveConf conf(String engine) {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_PLAN_SERIALIZATION_ENGINE, engine);
    return conf;
  }

  /** A structural fingerprint of an operator graph: class, child links and the printed descriptor. */
  private static String fingerprint(Operator<?> op, String indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(indent).append(op.getClass().getSimpleName()).append(' ').append(op.getConf() == null ? "" :
        op.getConf().getClass().getSimpleName()).append('\n');
    if (op.getChildOperators() != null) {
      for (Operator<?> child : op.getChildOperators()) {
        Assert.assertTrue("parent link not restored for " + child, child.getParentOperators().contains(op));
        sb.append(fingerprint(child, indent + "  "));
      }
    }
    return sb.toString();
  }

  private static String fingerprint(MapredWork w) {
    StringBuilder sb = new StringBuilder();
    for (Operator<?> root : w.getMapWork().getAliasToWork().values()) {
      sb.append("map:\n").append(fingerprint(root, "  "));
    }
    if (w.getReduceWork() != null && w.getReduceWork().getReducer() != null) {
      sb.append("reduce:\n").append(fingerprint(w.getReduceWork().getReducer(), "  "));
    }
    return sb.toString();
  }

  private static MapredWork roundTrip(String engine, MapredWork plan) {
    HiveConf conf = conf(engine);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(conf, plan, out);
    return SerializationUtilities.deserializePlanWithEngine(conf, new ByteArrayInputStream(out.toByteArray()),
        MapredWork.class);
  }

  private void check(String engine, MapredWork plan) {
    String before = fingerprint(plan);
    MapredWork after = roundTrip(engine, plan);
    Assert.assertEquals(engine, before, fingerprint(after));
  }

  @Test
  public void testMapRedPlanBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      check(engine, mapRedPlan());
    }
  }

  @Test
  public void testMapOnlyPlanWithUdfExpressionsBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      check(engine, mapOnlyPlan());
    }
  }

  @Test
  public void testFilterExpressionSurvivesForyRoundTrip() throws Exception {
    MapredWork after = roundTrip("fory", mapOnlyPlan());
    Operator<?> filter = after.getMapWork().getAliasToWork().get("a");
    FilterDesc fd = (FilterDesc) filter.getConf();
    Assert.assertEquals(filterDesc("key").getPredicate().getExprString(), fd.getPredicate().getExprString());
  }

  /** The production path: Utilities.setMapRedWork writes (file or conf string), getMapWork/getReduceWork read back. */
  private void checkViaUtilities(String engine, boolean rpcPlan) throws Exception {
    HiveConf conf = conf(engine);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, rpcPlan);
    conf.set(Utilities.MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    conf.set("mapred.reducer.class", ExecReducer.class.getName());
    conf.set("fs.defaultFS", "file:///");
    Path scratch = new Path(System.getProperty("java.io.tmpdir"), "plan-engines-" + engine + "-" + rpcPlan);
    MapredWork plan = mapRedPlan();
    String before = fingerprint(plan);

    Utilities.setMapRedWork(conf, plan, scratch);
    // Drop the in-process cache so that the plan is really deserialized from the file or the conf string.
    Utilities.clearWorkMap(conf);
    MapredWork read = new MapredWork();
    MapWork mapWork = Utilities.getMapWork(conf);
    Assert.assertNotNull(mapWork);
    read.setMapWork(mapWork);
    read.setReduceWork(Utilities.getReduceWork(conf));
    Assert.assertEquals(engine + " rpc=" + rpcPlan, before, fingerprint(read));
    Utilities.clearWork(conf);
  }

  @Test
  public void testUtilitiesWriteReadViaFileBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      checkViaUtilities(engine, false);
    }
  }

  @Test
  public void testUtilitiesWriteReadViaConfStringBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      checkViaUtilities(engine, true);
    }
  }

  /** Same shape as the LLAP UDF whitelist: rejects the listed classes in preRead and injects a checker in postRead. */
  private static final class RejectingHook extends SerializationUtilities.Hook {
    private final Class<?> denied;
    int postReadBridges;

    RejectingHook(Class<?> denied) {
      this.denied = denied;
    }

    @Override
    public boolean preRead(Class<?> type) {
      if (denied.equals(type)) {
        throw new SecurityException("UDF " + type.getCanonicalName() + " is not allowed");
      }
      return false;
    }

    @Override
    public Object postRead(Object o) {
      if (o instanceof org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge) {
        postReadBridges++;
      }
      return o;
    }
  }

  @Test
  public void testVectorizedRandWithSeedSurvivesBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      SerializationUtilities.serializePlanWithEngine(conf(engine),
          new org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRand(42L, 1), out);
      org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRand read = SerializationUtilities
          .deserializePlanWithEngine(conf(engine), new ByteArrayInputStream(out.toByteArray()),
              org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRand.class);
      java.lang.reflect.Field f = org.apache.hadoop.hive.ql.exec.vector.expressions.FuncRand.class
          .getDeclaredField("random");
      f.setAccessible(true);
      Assert.assertEquals(engine, new java.util.Random(42L).nextLong(), ((java.util.Random) f.get(read)).nextLong());
    }
  }

  @Test
  public void testForyGlobalHookRejectsDeniedUdfClass() throws Exception {
    MapredWork plan = mapOnlyPlan();
    HiveConf conf = conf("fory");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(conf, plan, out);
    SerializationUtilities.Hook before = SerializationUtilities.globalHook();
    SerializationUtilities.setGlobalHook(new RejectingHook(org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan.class));
    try {
      SerializationUtilities.deserializePlanWithEngine(conf, new ByteArrayInputStream(out.toByteArray()),
          MapredWork.class);
      Assert.fail("the denied UDF class must be rejected");
    } catch (RuntimeException e) {
      Throwable t = e;
      boolean rejected = false;
      while (t != null) {
        rejected |= t instanceof SecurityException || String.valueOf(t.getMessage()).contains("not allowed")
            || t instanceof org.apache.fory.exception.InsecureException;
        t = t.getCause();
      }
      Assert.assertTrue("unexpected failure: " + e, rejected);
    } finally {
      SerializationUtilities.setGlobalHook(before);
    }
  }

  @Test
  public void testForyGlobalHookAllowsOtherUdfsAndWritesAreUnaffected() throws Exception {
    MapredWork plan = mapOnlyPlan();
    HiveConf conf = conf("fory");
    String expected = fingerprint(plan);
    SerializationUtilities.Hook before = SerializationUtilities.globalHook();
    SerializationUtilities.setGlobalHook(new RejectingHook(org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan.class));
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      SerializationUtilities.serializePlanWithEngine(conf, plan, out);
      MapredWork back = SerializationUtilities.deserializePlanWithEngine(conf,
          new ByteArrayInputStream(out.toByteArray()), MapredWork.class);
      Assert.assertEquals(expected, fingerprint(back));
    } finally {
      SerializationUtilities.setGlobalHook(before);
    }
  }
}
