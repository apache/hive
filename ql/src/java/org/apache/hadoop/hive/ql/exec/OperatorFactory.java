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

import java.util.*;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.*;

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
    opvec = new ArrayList<opTuple> ();
    opvec.add(new opTuple<filterDesc> (filterDesc.class, FilterOperator.class));
    opvec.add(new opTuple<selectDesc> (selectDesc.class, SelectOperator.class));
    opvec.add(new opTuple<forwardDesc> (forwardDesc.class, ForwardOperator.class));
    opvec.add(new opTuple<fileSinkDesc> (fileSinkDesc.class, FileSinkOperator.class));
    opvec.add(new opTuple<collectDesc> (collectDesc.class, CollectOperator.class));
    opvec.add(new opTuple<scriptDesc> (scriptDesc.class, ScriptOperator.class));
    opvec.add(new opTuple<reduceSinkDesc> (reduceSinkDesc.class, ReduceSinkOperator.class));
    opvec.add(new opTuple<extractDesc> (extractDesc.class, ExtractOperator.class));
    opvec.add(new opTuple<groupByDesc> (groupByDesc.class, GroupByOperator.class));
    opvec.add(new opTuple<joinDesc> (joinDesc.class, JoinOperator.class));
    opvec.add(new opTuple<mapJoinDesc> (mapJoinDesc.class, MapJoinOperator.class));
    opvec.add(new opTuple<limitDesc> (limitDesc.class, LimitOperator.class));
    opvec.add(new opTuple<tableScanDesc> (tableScanDesc.class, TableScanOperator.class));
    opvec.add(new opTuple<unionDesc> (unionDesc.class, UnionOperator.class));
    opvec.add(new opTuple<udtfDesc> (udtfDesc.class, UDTFOperator.class));
  }
              

  public static <T extends Serializable> Operator<T> get(Class<T> opClass) {
      
    for(opTuple o: opvec) {
      if(o.descClass == opClass) {
        try {
          Operator<T> op = (Operator<T>)o.opClass.newInstance();
          op.initializeCounters();
          return op;
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException ("No operator for descriptor class " + opClass.getName());
  }

  public static <T extends Serializable> Operator<T> get(Class<T> opClass, RowSchema rwsch) {
    
    Operator<T> ret = get(opClass);
    ret.setSchema(rwsch);
    return ret;
  }

  /**
   * Returns an operator given the conf and a list of children operators.  
   */
  public static <T extends Serializable> Operator<T> get(T conf, Operator<? extends Serializable> ... oplist) {
    Operator<T> ret = get((Class <T>)conf.getClass());
    ret.setConf(conf);
    if(oplist.length == 0)
      return (ret);

    ArrayList<Operator<? extends Serializable>> clist = new ArrayList<Operator<? extends Serializable>> ();
    for(Operator op: oplist) {
      clist.add(op);
    }
    ret.setChildOperators(clist);
    
    // Add this parent to the children
    for(Operator op: oplist) {
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
  public static <T extends Serializable> Operator<T> get(T conf, RowSchema rwsch, Operator ... oplist) {
    Operator<T> ret = get(conf, oplist);
    ret.setSchema(rwsch);
    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.  
   */
  public static <T extends Serializable> Operator<T> getAndMakeChild(T conf, Operator ... oplist) {
    Operator<T> ret = get((Class <T>)conf.getClass());
    ret.setConf(conf);
    if(oplist.length == 0)
      return (ret);

    // Add the new operator as child of each of the passed in operators
    for(Operator op: oplist) {
      List<Operator> children = op.getChildOperators();
      if (children == null) {
        children = new ArrayList<Operator>();
      }
      children.add(ret);
      op.setChildOperators(children);
    }

    // add parents for the newly created operator
    List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
    for(Operator op: oplist)
      parent.add(op);
    
    ret.setParentOperators(parent);

    return (ret);
  }

  /**
   * Returns an operator given the conf and a list of parent operators.  
   */
  public static <T extends Serializable> Operator<T> getAndMakeChild(T conf, RowSchema rwsch, Operator ... oplist) {
    Operator<T> ret = getAndMakeChild(conf, oplist);
    ret.setSchema(rwsch);
    return (ret);
  }

}
