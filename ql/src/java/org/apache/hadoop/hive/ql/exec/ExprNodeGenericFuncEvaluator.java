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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

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
  transient ObjectInspector outputOI;
  transient ExprNodeEvaluator[] children;
  transient GenericUDF.DeferredObject[] deferredChildren;
  transient boolean isEager;

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
  }

  /**
   * Class to force eager evaluation for GenericUDF in cases where
   * it is warranted.
   */
  class EagerExprObject implements GenericUDF.DeferredObject {

    ExprNodeEvaluator eval;

    transient Object obj;

    EagerExprObject(ExprNodeEvaluator eval) {
      this.eval = eval;
    }

    void evaluate() throws HiveException {
      obj = eval.evaluate(rowObject);
    }
    
    public Object get() throws HiveException {
      return obj;
    }
  }
  
  public ExprNodeGenericFuncEvaluator(ExprNodeGenericFuncDesc expr) {
    this.expr = expr;
    children = new ExprNodeEvaluator[expr.getChildExprs().size()];
    isEager = false;
    for (int i = 0; i < children.length; i++) {
      ExprNodeDesc child = expr.getChildExprs().get(i);
      ExprNodeEvaluator nodeEvaluator = ExprNodeEvaluatorFactory.get(child);
      children[i] = nodeEvaluator;
      // If we have eager evaluators anywhere below us, then we are eager too.
      if (nodeEvaluator instanceof ExprNodeGenericFuncEvaluator) {
        if (((ExprNodeGenericFuncEvaluator) nodeEvaluator).isEager) {
          isEager = true;
        }
        // Base case:  we are eager if a child is stateful
        GenericUDF childUDF =
          ((ExprNodeGenericFuncDesc) child).getGenericUDF();
        if (FunctionRegistry.isStateful(childUDF)) {
          isEager = true;
        }
      }
    }
    deferredChildren =
      new GenericUDF.DeferredObject[expr.getChildExprs().size()];
    for (int i = 0; i < deferredChildren.length; i++) {
      if (isEager) {
        deferredChildren[i] = new EagerExprObject(children[i]);
      } else {
        deferredChildren[i] = new DeferredExprObject(children[i]);
      }
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
    if (isEager &&
      ((genericUDF instanceof GenericUDFCase)
        || (genericUDF instanceof GenericUDFWhen))) {
      throw new HiveException(
        "Stateful expressions cannot be used inside of CASE");
    }
    this.outputOI = genericUDF.initializeAndFoldConstants(childrenOIs);
    return this.outputOI;
  }

  @Override
  public Object evaluate(Object row) throws HiveException {
    rowObject = row;
    if (ObjectInspectorUtils.isConstantObjectInspector(outputOI)) {
      // The output of this UDF is constant, so don't even bother evaluating.
      return ((ConstantObjectInspector)outputOI).getWritableConstantValue();
    }
    if (isEager) {
      for (int i = 0; i < deferredChildren.length; i++) {
        ((EagerExprObject) deferredChildren[i]).evaluate();
      }
    }
    return genericUDF.evaluate(deferredChildren);
  }

}
