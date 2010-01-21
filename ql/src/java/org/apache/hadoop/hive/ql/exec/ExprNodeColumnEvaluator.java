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
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * This evaluator gets the column from the row object.
 */
public class ExprNodeColumnEvaluator extends ExprNodeEvaluator {

  protected exprNodeColumnDesc expr;

  transient StructObjectInspector[] inspectors;
  transient StructField[] fields;

  public ExprNodeColumnEvaluator(exprNodeColumnDesc expr) {
    this.expr = expr;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector)
      throws HiveException {

    // We need to support field names like KEY.0, VALUE.1 between
    // map-reduce boundary.
    String[] names = expr.getColumn().split("\\.");
    inspectors = new StructObjectInspector[names.length];
    fields = new StructField[names.length];

    for (int i = 0; i < names.length; i++) {
      if (i == 0) {
        inspectors[0] = (StructObjectInspector) rowInspector;
      } else {
        inspectors[i] = (StructObjectInspector) fields[i - 1]
            .getFieldObjectInspector();
      }
      fields[i] = inspectors[i].getStructFieldRef(names[i]);
    }
    return fields[names.length - 1].getFieldObjectInspector();
  }

  @Override
  public Object evaluate(Object row) throws HiveException {
    Object o = row;
    for (int i = 0; i < fields.length; i++) {
      o = inspectors[i].getStructFieldData(o, fields[i]);
    }
    return o;
  }

}
