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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class ExprNodeConstantEvaluator extends ExprNodeEvaluator {

  protected exprNodeConstantDesc expr;
  transient ObjectInspector writableObjectInspector;
  transient Object writableValue;

  public ExprNodeConstantEvaluator(exprNodeConstantDesc expr) {
    this.expr = expr;
    PrimitiveCategory pc = ((PrimitiveTypeInfo) expr.getTypeInfo())
        .getPrimitiveCategory();
    writableObjectInspector = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(pc);
    // Convert from Java to Writable
    writableValue = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(pc).getPrimitiveWritableObject(
            expr.getValue());
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector)
      throws HiveException {
    return writableObjectInspector;
  }

  @Override
  public Object evaluate(Object row) throws HiveException {
    return writableValue;
  }

}
