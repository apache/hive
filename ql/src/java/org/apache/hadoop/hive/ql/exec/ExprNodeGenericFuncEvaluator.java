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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * ExprNodeGenericFuncEvaluator.
 *
 */
public class ExprNodeGenericFuncEvaluator extends ExprNodeEvaluator {

  private static final Log LOG = LogFactory
      .getLog(ExprNodeGenericFuncEvaluator.class.getName());

  protected ExprNodeGenericFuncDesc expr;

  transient GenericUDF genericUDF;
  transient Object rowObject;
  transient ExprNodeEvaluator[] children;
  transient DeferredExprObject[] deferredChildren;

  /**
   * Class to allow deferred evaluation for GenericUDF.
   */
  class DeferredExprObject implements GenericUDF.DeferredObject {

    ExprNodeEvaluator eval;

    DeferredExprObject(ExprNodeEvaluator eval) {
      this.eval = eval;
    }

    public Object get() throws HiveException {
      return eval.evaluate(rowObject);
    }
  };

  public ExprNodeGenericFuncEvaluator(ExprNodeGenericFuncDesc expr) {
    this.expr = expr;
    children = new ExprNodeEvaluator[expr.getChildExprs().size()];
    for (int i = 0; i < children.length; i++) {
      children[i] = ExprNodeEvaluatorFactory.get(expr.getChildExprs().get(i));
    }
    deferredChildren = new DeferredExprObject[expr.getChildExprs().size()];
    for (int i = 0; i < deferredChildren.length; i++) {
      deferredChildren[i] = new DeferredExprObject(children[i]);
    }
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {
    // Initialize all children first
    ObjectInspector[] childrenOIs = new ObjectInspector[children.length];
    for (int i = 0; i < children.length; i++) {
      childrenOIs[i] = children[i].initialize(rowInspector);
    }
    genericUDF = expr.getGenericUDF();
    return genericUDF.initialize(childrenOIs);
  }

  @Override
  public Object evaluate(Object row) throws HiveException {
    rowObject = row;
    return genericUDF.evaluate(deferredChildren);
  }

}
