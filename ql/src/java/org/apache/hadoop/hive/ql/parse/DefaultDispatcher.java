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
import java.util.List;
import java.util.Stack;
import java.lang.ClassNotFoundException;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorFactory.opTuple;

/**
 * Dispatches calls to relevant method in processor 
 */
public class DefaultDispatcher implements Dispatcher {
  
  private OperatorProcessor opProcessor;
  
  /**
   * constructor
   * @param opp operator processor that handles actual processing of the node
   */
  public DefaultDispatcher(OperatorProcessor opp) {
    this.opProcessor = opp;
  }

  /**
   * dispatcher function
   * @param op operator to process
   * @param opStack the operators encountered so far
   * @throws SemanticException
   */
  public void dispatch(Operator<? extends Serializable> op, Stack<Operator<? extends Serializable>> opStack) 
    throws SemanticException {

    // If the processor has registered a process method for the particular operator, invoke it.
    // Otherwise implement the generic function, which would definitely be implemented
    for(opTuple opt : OperatorFactory.opvec) {
      if(opt.opClass.isInstance(op)) {
        Method pcall;
        try {
          pcall = opProcessor.getClass().getMethod("process", opt.opClass, 
                                                   Class.forName("org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext"));
          pcall.invoke(opProcessor, op, null);
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
      Method pcall = opProcessor.getClass().getMethod("process", Class.forName("org.apache.hadoop.hive.ql.exec.Operator"), 
                                                      Class.forName("org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext"));
      
      pcall.invoke(opProcessor, ((Operator<? extends Serializable>)op), null);
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
