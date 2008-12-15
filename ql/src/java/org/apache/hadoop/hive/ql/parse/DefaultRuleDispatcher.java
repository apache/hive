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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.ClassNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.lang.ClassNotFoundException;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorFactory.opTuple;

/**
 * Dispatches calls to relevant method in processor. The user registers various rules with the dispatcher, and
 * the processor corresponding to closest matching rule is fired.
 */
public class DefaultRuleDispatcher implements Dispatcher {
  
  private Map<Rule, OperatorProcessor>  opProcRules;
  private OperatorProcessorContext      opProcCtx;
  private OperatorProcessor             defaultProc;

  /**
   * constructor
   * @param defaultProc defualt processor to be fired if no rule matches
   * @param opp operator processor that handles actual processing of the node
   * @param opProcCtx operator processor context, which is opaque to the dispatcher
   */
  public DefaultRuleDispatcher(OperatorProcessor defaultProc, 
                               Map<Rule, OperatorProcessor> opp, OperatorProcessorContext opProcCtx) {
    this.defaultProc = defaultProc;
    this.opProcRules = opp;
    this.opProcCtx   = opProcCtx;
  }

  /**
   * dispatcher function
   * @param op operator to process
   * @param opStack the operators encountered so far
   * @throws SemanticException
   */
  public void dispatch(Operator<? extends Serializable> op, Stack<Operator<? extends Serializable>> opStack) 
    throws SemanticException {

    // find the firing rule
    // find the rule from the stack specified
    Rule rule = null;
    int minCost = Integer.MAX_VALUE;
    for (Rule r : opProcRules.keySet()) {
      int cost = r.cost(opStack);
      if ((cost >= 0) && (cost <= minCost)) {
        minCost = cost;
        rule = r;
      }
    }

    OperatorProcessor proc;

    if (rule == null)
      proc = defaultProc;
    else
      proc = opProcRules.get(rule);

    // If the processor has registered a process method for the particular operator, invoke it.
    // Otherwise implement the generic function, which would definitely be implemented
    for(opTuple opt : OperatorFactory.opvec) {
      if(opt.opClass.isInstance(op)) {
        Method pcall;
        try {
          pcall = proc.getClass().getMethod("process", opt.opClass,
                                            Class.forName("org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext"));
          pcall.invoke(proc, op, opProcCtx);
          return;
        } catch (SecurityException e) {
          assert false;
        } catch (NoSuchMethodException e) {
          assert false;
        } catch (IllegalArgumentException e) {
          assert false;
        } catch (IllegalAccessException e) {
          assert false;
        } catch (InvocationTargetException e) {
          throw new SemanticException(e.getTargetException());
        } catch (ClassNotFoundException e) {
          assert false;
        }
      }
    }

    try {
      // no method found - invoke the generic function
      Method pcall = proc.getClass().getMethod("process", Class.forName("org.apache.hadoop.hive.ql.exec.Operator"), 
                                               Class.forName("org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext"));
      pcall.invoke(proc, ((Operator<? extends Serializable>)op), opProcCtx);

    } catch (SecurityException e) {
      assert false;
    } catch (NoSuchMethodException e) {
      assert false;
    } catch (IllegalArgumentException e) {
      assert false;
    } catch (IllegalAccessException e) {
      assert false;
    } catch (InvocationTargetException e) {
      throw new SemanticException(e.getTargetException());
    } catch (ClassNotFoundException e) {
      assert false;
    }
  }
}
