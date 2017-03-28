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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorAppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkCommonOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.AbstractVectorDesc;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.DemuxDesc;
import org.apache.hadoop.hive.ql.plan.DummyStoreDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MuxDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;

import com.google.common.base.Preconditions;

/**
 * OperatorFactory.
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class OperatorFactory {
  protected static transient final Logger LOG = LoggerFactory.getLogger(OperatorFactory.class);
  private static final IdentityHashMap<Class<? extends OperatorDesc>,
    Class<? extends Operator<? extends OperatorDesc>>> opvec = new IdentityHashMap<>();
  private static final IdentityHashMap<Class<? extends OperatorDesc>,
    Class<? extends Operator<? extends OperatorDesc>>> vectorOpvec = new IdentityHashMap<>();

  static {
    opvec.put(FilterDesc.class, FilterOperator.class);
    opvec.put(SelectDesc.class, SelectOperator.class);
    opvec.put(ForwardDesc.class, ForwardOperator.class);
    opvec.put(FileSinkDesc.class, FileSinkOperator.class);
    opvec.put(CollectDesc.class, CollectOperator.class);
    opvec.put(ScriptDesc.class, ScriptOperator.class);
    opvec.put(PTFDesc.class, PTFOperator.class);
    opvec.put(ReduceSinkDesc.class, ReduceSinkOperator.class);
    opvec.put(GroupByDesc.class, GroupByOperator.class);
    opvec.put(JoinDesc.class, JoinOperator.class);
    opvec.put(MapJoinDesc.class, MapJoinOperator.class);
    opvec.put(SMBJoinDesc.class, SMBMapJoinOperator.class);
    opvec.put(LimitDesc.class, LimitOperator.class);
    opvec.put(TableScanDesc.class, TableScanOperator.class);
    opvec.put(UnionDesc.class, UnionOperator.class);
    opvec.put(UDTFDesc.class, UDTFOperator.class);
    opvec.put(LateralViewJoinDesc.class, LateralViewJoinOperator.class);
    opvec.put(LateralViewForwardDesc.class, LateralViewForwardOperator.class);
    opvec.put(HashTableDummyDesc.class, HashTableDummyOperator.class);
    opvec.put(HashTableSinkDesc.class, HashTableSinkOperator.class);
    opvec.put(SparkHashTableSinkDesc.class, SparkHashTableSinkOperator.class);
    opvec.put(DummyStoreDesc.class, DummyStoreOperator.class);
    opvec.put(DemuxDesc.class, DemuxOperator.class);
    opvec.put(MuxDesc.class, MuxOperator.class);
    opvec.put(AppMasterEventDesc.class, AppMasterEventOperator.class);
    opvec.put(DynamicPruningEventDesc.class, AppMasterEventOperator.class);
    opvec.put(SparkPartitionPruningSinkDesc.class, SparkPartitionPruningSinkOperator.class);
    opvec.put(RCFileMergeDesc.class, RCFileMergeOperator.class);
    opvec.put(OrcFileMergeDesc.class, OrcFileMergeOperator.class);
    opvec.put(CommonMergeJoinDesc.class, CommonMergeJoinOperator.class);
    opvec.put(ListSinkDesc.class, ListSinkOperator.class);
  }

  static {
    vectorOpvec.put(AppMasterEventDesc.class, VectorAppMasterEventOperator.class);
    vectorOpvec.put(DynamicPruningEventDesc.class, VectorAppMasterEventOperator.class);
    vectorOpvec.put(
        SparkPartitionPruningSinkDesc.class, VectorSparkPartitionPruningSinkOperator.class);
    vectorOpvec.put(SelectDesc.class, VectorSelectOperator.class);
    vectorOpvec.put(GroupByDesc.class, VectorGroupByOperator.class);
    vectorOpvec.put(MapJoinDesc.class, VectorMapJoinOperator.class);
    vectorOpvec.put(SMBJoinDesc.class, VectorSMBMapJoinOperator.class);
    vectorOpvec.put(ReduceSinkDesc.class, VectorReduceSinkOperator.class);
    vectorOpvec.put(FileSinkDesc.class, VectorFileSinkOperator.class);
    vectorOpvec.put(FilterDesc.class, VectorFilterOperator.class);
    vectorOpvec.put(LimitDesc.class, VectorLimitOperator.class);
    vectorOpvec.put(SparkHashTableSinkDesc.class, VectorSparkHashTableSinkOperator.class);
  }

  public static <T extends OperatorDesc> Operator<T> getVectorOperator(
    Class<? extends Operator<?>> opClass, CompilationOpContext cContext, T conf,
        VectorizationContext vContext, Operator<? extends OperatorDesc> originalOp) throws HiveException {
    try {
      VectorDesc vectorDesc = ((AbstractOperatorDesc) conf).getVectorDesc();
      vectorDesc.setVectorOp(opClass);
      Operator<T> op = (Operator<T>) opClass.getDeclaredConstructor(CompilationOpContext.class,
          VectorizationContext.class, OperatorDesc.class).newInstance(cContext, vContext, conf);
      op.setOperatorId(originalOp.getOperatorId());
      if (op instanceof VectorReduceSinkOperator || op instanceof VectorReduceSinkCommonOperator) {
        ((ReduceSinkDesc) op.getConf()).setOutputOperators(((ReduceSinkDesc) originalOp.getConf())
            .getOutputOperators());
      }
      return op;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public static <T extends OperatorDesc> Operator<T> getVectorOperator(
      CompilationOpContext cContext, T conf, VectorizationContext vContext,
      Operator<? extends OperatorDesc> originalOp) throws HiveException {
    Class<T> descClass = (Class<T>) conf.getClass();
    Class<? extends Operator<? extends OperatorDesc>> opClass = vectorOpvec.get(descClass);
    if (opClass != null) {
      return getVectorOperator(opClass, cContext, conf, vContext, originalOp);
    }
    throw new HiveException("No vector operator for descriptor class " + descClass.getName());
  }

  public static <T extends OperatorDesc> Operator<T> get(
      CompilationOpContext cContext, Class<T> descClass) {
    Preconditions.checkNotNull(cContext);
    Class<?> opClass = opvec.get(descClass);
    if (opClass != null) {
      try {
        return (Operator<T>)opClass.getDeclaredConstructor(
          CompilationOpContext.class).newInstance(cContext);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("No operator for descriptor class " + descClass.getName());
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static <T extends OperatorDesc> Operator<T> get(CompilationOpContext cContext, T conf) {
    Operator<T> ret = get(cContext, (Class<T>) conf.getClass());
    ret.setConf(conf);
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static <T extends OperatorDesc> Operator<T> get(T conf,
    Operator<? extends OperatorDesc> oplist0, Operator<? extends OperatorDesc>... oplist) {
    Operator<T> ret = get(oplist0.getCompilationOpContext(), (Class<T>) conf.getClass());
    ret.setConf(conf);
    makeChild(ret, oplist0);
    makeChild(ret, oplist);
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static void makeChild(
    Operator<? extends OperatorDesc> ret,
    Operator<? extends OperatorDesc>... oplist) {
    if (oplist.length == 0) {
      return;
    }

    ArrayList<Operator<? extends OperatorDesc>> clist =
      new ArrayList<Operator<? extends OperatorDesc>>();
    for (Operator<? extends OperatorDesc> op : oplist) {
      clist.add(op);
    }
    ret.setChildOperators(clist);

    // Add this parent to the children
    for (Operator<? extends OperatorDesc> op : oplist) {
      List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
      parents.add(ret);
      op.setParentOperators(parents);
    }
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static <T extends OperatorDesc> Operator<T> get(
      CompilationOpContext cContext, T conf, RowSchema rwsch) {
    Operator<T> ret = get(cContext, conf);
    ret.setSchema(rwsch);
    return (ret);
  }


  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(
      T conf, Operator oplist0, Operator... oplist) {
    Operator<T> ret = get(oplist0.getCompilationOpContext(), (Class<T>) conf.getClass());
    ret.setConf(conf);

    // Add the new operator as child of each of the passed in operators
    List<Operator> children = oplist0.getChildOperators();
    children.add(ret);
    oplist0.setChildOperators(children);
    for (Operator op : oplist) {
      children = op.getChildOperators();
      children.add(ret);
      op.setChildOperators(children);
    }

    // add parents for the newly created operator
    List<Operator<? extends OperatorDesc>> parent =
      new ArrayList<Operator<? extends OperatorDesc>>();
    parent.add(oplist0);
    for (Operator op : oplist) {
      parent.add(op);
    }

    ret.setParentOperators(parent);

    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(CompilationOpContext cContext,
      T conf, List<Operator<? extends OperatorDesc>> oplist) {
    Operator<T> ret = get(cContext, (Class<T>) conf.getClass());
    ret.setConf(conf);
    if (oplist.size() == 0) {
      return ret;
    }

    // Add the new operator as child of each of the passed in operators
    for (Operator op : oplist) {
      List<Operator> children = op.getChildOperators();
      children.add(ret);
    }

    // add parents for the newly created operator
    List<Operator<? extends OperatorDesc>> parent =
      new ArrayList<Operator<? extends OperatorDesc>>();
    for (Operator op : oplist) {
      parent.add(op);
    }

    ret.setParentOperators(parent);

    return ret;
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(
      CompilationOpContext cContext, T conf, RowSchema rwsch) {
    Operator<T> ret = get(cContext, (Class<T>) conf.getClass());
    ret.setConf(conf);
    ret.setSchema(rwsch);
    return ret;
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(
      CompilationOpContext ctx, T conf, RowSchema rwsch, Operator[] oplist) {
    Operator<T> ret = get(ctx, (Class<T>) conf.getClass());
    ret.setConf(conf);
    ret.setSchema(rwsch);
    if (oplist.length == 0) return ret;

    // Add the new operator as child of each of the passed in operators
    for (Operator op : oplist) {
      List<Operator> children = op.getChildOperators();
      children.add(ret);
      op.setChildOperators(children);
    }

    // add parents for the newly created operator
    List<Operator<? extends OperatorDesc>> parent =
      new ArrayList<Operator<? extends OperatorDesc>>();
    for (Operator op : oplist) {
      parent.add(op);
    }

    ret.setParentOperators(parent);

    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(
      T conf, RowSchema rwsch, Operator oplist0, Operator... oplist) {
    Operator<T> ret = getAndMakeChild(conf, oplist0, oplist);
    ret.setSchema(rwsch);
    return ret;
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(T conf, RowSchema rwsch,
      Map<String, ExprNodeDesc> colExprMap, Operator oplist0, Operator... oplist) {
    Operator<T> ret = getAndMakeChild(conf, rwsch, oplist0, oplist);
    ret.setColumnExprMap(colExprMap);
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(CompilationOpContext cContext,
      T conf, RowSchema rwsch, List<Operator<? extends OperatorDesc>> oplist) {
    Operator<T> ret = getAndMakeChild(cContext, conf, oplist);
    ret.setSchema(rwsch);
    return (ret);
  }

 /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends OperatorDesc> Operator<T> getAndMakeChild(CompilationOpContext cContext,
      T conf, RowSchema rwsch, Map<String, ExprNodeDesc> colExprMap,
      List<Operator<? extends OperatorDesc>> oplist) {
    Operator<T> ret = getAndMakeChild(cContext, conf, rwsch, oplist);
    ret.setColumnExprMap(colExprMap);
    return (ret);
  }

  private OperatorFactory() {
    // prevent instantiation
  }
}
