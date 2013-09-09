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
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;

/**
 * This evaluator gets the column from the row object.
 */
public class ExprNodeColumnEvaluator extends ExprNodeEvaluator<ExprNodeColumnDesc> {

  private transient boolean simpleCase;
  private transient StructObjectInspector inspector;
  private transient StructField field;
  private transient StructObjectInspector[] inspectors;
  private transient StructField[] fields;
  private transient boolean[] unionField;

  public ExprNodeColumnEvaluator(ExprNodeColumnDesc expr) {
    super(expr);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {

    // We need to support field names like KEY.0, VALUE.1 between
    // map-reduce boundary.
    String[] names = expr.getColumn().split("\\.");
    String[] unionfields = names[0].split("\\:");
    if (names.length == 1 && unionfields.length == 1) {
      simpleCase = true;
      inspector = (StructObjectInspector) rowInspector;
      field = inspector.getStructFieldRef(names[0]);
      return outputOI = field.getFieldObjectInspector();
    }

    simpleCase = false;
    inspectors = new StructObjectInspector[names.length];
    fields = new StructField[names.length];
    unionField = new boolean[names.length];
    int unionIndex = -1;
    for (int i = 0; i < names.length; i++) {
      if (i == 0) {
        inspectors[0] = (StructObjectInspector) rowInspector;
      } else {
        if (unionIndex == -1) {
          inspectors[i] = (StructObjectInspector) fields[i - 1]
              .getFieldObjectInspector();
        } else {
          inspectors[i] = (StructObjectInspector) (
              (UnionObjectInspector)fields[i-1].getFieldObjectInspector()).
              getObjectInspectors().get(unionIndex);
        }
      }
      // to support names like _colx:1._coly
      unionfields = names[i].split("\\:");
      fields[i] = inspectors[i].getStructFieldRef(unionfields[0]);
      if (unionfields.length > 1) {
        unionIndex = Integer.parseInt(unionfields[1]);
        unionField[i] = true;
      } else {
        unionIndex = -1;
        unionField[i] = false;
      }
    }
    return outputOI = fields[names.length - 1].getFieldObjectInspector();
  }

  @Override
  protected Object _evaluate(Object row, int version) throws HiveException {
    if (simpleCase) {
      return inspector.getStructFieldData(row, field);
    }
    Object o = row;
    for (int i = 0; i < fields.length; i++) {
      o = inspectors[i].getStructFieldData(o, fields[i]);
      if (unionField[i]) {
        o = ((StandardUnion)o).getObject();
      }
    }
    return o;
  }

}
