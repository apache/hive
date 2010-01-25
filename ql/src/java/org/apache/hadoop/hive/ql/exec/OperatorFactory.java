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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;

public class OperatorFactory {

  public final static class opTuple<T extends Serializable> {
    public Class<T> descClass;
    public Class<? extends Operator<T>> opClass;

    public opTuple(Class<T> descClass, Class<? extends Operator<T>> opClass) {
      this.descClass = descClass;
      this.opClass = opClass;
    }
  }

  public static ArrayList<opTuple> opvec;
  static {
    opvec = new ArrayList<opTuple>();
    opvec.add(new opTuple<FilterDesc>(FilterDesc.class, FilterOperator.class));
    opvec.add(new opTuple<SelectDesc>(SelectDesc.class, SelectOperator.class));
    opvec
        .add(new opTuple<ForwardDesc>(ForwardDesc.class, ForwardOperator.class));
    opvec.add(new opTuple<FileSinkDesc>(FileSinkDesc.class,
        FileSinkOperator.class));
    opvec
        .add(new opTuple<CollectDesc>(CollectDesc.class, CollectOperator.class));
    opvec.add(new opTuple<ScriptDesc>(ScriptDesc.class, ScriptOperator.class));
    opvec.add(new opTuple<ReduceSinkDesc>(ReduceSinkDesc.class,
        ReduceSinkOperator.class));
    opvec
        .add(new opTuple<ExtractDesc>(ExtractDesc.class, ExtractOperator.class));
    opvec
        .add(new opTuple<GroupByDesc>(GroupByDesc.class, GroupByOperator.class));
    opvec.add(new opTuple<JoinDesc>(JoinDesc.class, JoinOperator.class));
    opvec
        .add(new opTuple<MapJoinDesc>(MapJoinDesc.class, MapJoinOperator.class));
    opvec.add(new opTuple<LimitDesc>(LimitDesc.class, LimitOperator.class));
    opvec.add(new opTuple<TableScanDesc>(TableScanDesc.class,
        TableScanOperator.class));
    opvec.add(new opTuple<UnionDesc>(UnionDesc.class, UnionOperator.class));
    opvec.add(new opTuple<UDTFDesc>(UDTFDesc.class, UDTFOperator.class));
    opvec.add(new opTuple<LateralViewJoinDesc>(LateralViewJoinDesc.class,
        LateralViewJoinOperator.class));
  }

  public static <T extends Serializable> Operator<T> get(Class<T> opClass) {

    for (opTuple o : opvec) {
      if (o.descClass == opClass) {
        try {
          Operator<T> op = (Operator<T>) o.opClass.newInstance();
          op.initializeCounters();
          return op;
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException("No operator for descriptor class "
        + opClass.getName());
  }

  public static <T extends Serializable> Operator<T> get(Class<T> opClass,
      RowSchema rwsch) {

    Operator<T> ret = get(opClass);
    ret.setSchema(rwsch);
    return ret;
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static <T extends Serializable> Operator<T> get(T conf,
      Operator<? extends Serializable>... oplist) {
    Operator<T> ret = get((Class<T>) conf.getClass());
    ret.setConf(conf);
    if (oplist.length == 0) {
      return (ret);
    }

    ArrayList<Operator<? extends Serializable>> clist = new ArrayList<Operator<? extends Serializable>>();
    for (Operator op : oplist) {
      clist.add(op);
    }
    ret.setChildOperators(clist);

    // Add this parent to the children
    for (Operator op : oplist) {
      List<Operator<? extends Serializable>> parents = op.getParentOperators();
      if (parents == null) {
        parents = new ArrayList<Operator<? extends Serializable>>();
      }
      parents.add(ret);
      op.setParentOperators(parents);
    }
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of children operators.
   */
  public static <T extends Serializable> Operator<T> get(T conf,
      RowSchema rwsch, Operator... oplist) {
    Operator<T> ret = get(conf, oplist);
    ret.setSchema(rwsch);
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends Serializable> Operator<T> getAndMakeChild(T conf,
      Operator... oplist) {
    Operator<T> ret = get((Class<T>) conf.getClass());
    ret.setConf(conf);
    if (oplist.length == 0) {
      return (ret);
    }

    // Add the new operator as child of each of the passed in operators
    for (Operator op : oplist) {
      List<Operator> children = op.getChildOperators();
      if (children == null) {
        children = new ArrayList<Operator>();
      }
      children.add(ret);
      op.setChildOperators(children);
    }

    // add parents for the newly created operator
    List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
    for (Operator op : oplist) {
      parent.add(op);
    }

    ret.setParentOperators(parent);

    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.
   */
  public static <T extends Serializable> Operator<T> getAndMakeChild(T conf,
      RowSchema rwsch, Operator... oplist) {
    Operator<T> ret = getAndMakeChild(conf, oplist);
    ret.setSchema(rwsch);
    return (ret);
  }

}
