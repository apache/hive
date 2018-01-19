/*
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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * This Evaluator can evaluate s.f for s as both struct and list of struct. If s
 * is struct, then s.f is the field. If s is list of struct, then s.f is the
 * list of struct field.
 */
public class ExprNodeFieldEvaluator extends ExprNodeEvaluator<ExprNodeFieldDesc> {

  transient ExprNodeEvaluator leftEvaluator;
  transient ObjectInspector leftInspector;
  transient StructObjectInspector structObjectInspector;
  transient StructField field;
  transient ObjectInspector structFieldObjectInspector;
  transient ObjectInspector resultObjectInspector;

  public ExprNodeFieldEvaluator(ExprNodeFieldDesc desc, Configuration conf) throws HiveException {
    super(desc, conf);
    leftEvaluator = ExprNodeEvaluatorFactory.get(desc.getDesc(), conf);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {
    leftInspector = leftEvaluator.initialize(rowInspector);
    if (expr.getIsList()) {
      structObjectInspector = (StructObjectInspector) ((ListObjectInspector) leftInspector)
          .getListElementObjectInspector();
    } else {
      structObjectInspector = (StructObjectInspector) leftInspector;
    }
    field = structObjectInspector.getStructFieldRef(expr.getFieldName());
    structFieldObjectInspector = field.getFieldObjectInspector();

    if (expr.getIsList()) {
      resultObjectInspector = ObjectInspectorFactory
          .getStandardListObjectInspector(structFieldObjectInspector);
    } else {
      resultObjectInspector = structFieldObjectInspector;
    }
    return outputOI = resultObjectInspector;
  }

  private List<Object> cachedList = new ArrayList<Object>();

  @Override
  protected Object _evaluate(Object row, int version) throws HiveException {

    // Get the result in leftInspectableObject
    Object left = leftEvaluator.evaluate(row, version);

    if (expr.getIsList()) {
      List<?> list = ((ListObjectInspector) leftInspector).getList(left);
      if (list == null) {
        return null;
      } else {
        cachedList.clear();
        for (int i = 0; i < list.size(); i++) {
          cachedList.add(structObjectInspector.getStructFieldData(list.get(i),
              field));
        }
        return cachedList;
      }
    } else {
      return structObjectInspector.getStructFieldData(left, field);
    }
  }

}
