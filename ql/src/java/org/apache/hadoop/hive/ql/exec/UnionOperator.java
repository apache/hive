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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Union Operator Just forwards. Doesn't do anything itself.
 **/
public class UnionOperator extends Operator<UnionDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  StructObjectInspector[] parentObjInspectors;
  List<? extends StructField>[] parentFields;

  ReturnObjectInspectorResolver[] columnTypeResolvers;
  boolean[] needsTransform;

  ArrayList<Object> outputRow;

  /** Kryo ctor. */
  protected UnionOperator() {
    super();
  }

  public UnionOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  /**
   * UnionOperator will transform the input rows if the inputObjInspectors from
   * different parents are different. If one parent has exactly the same
   * ObjectInspector as the output ObjectInspector, then we don't need to do
   * transformation for that parent. This information is recorded in
   * needsTransform[].
   */
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    int parents = parentOperators.size();
    parentObjInspectors = new StructObjectInspector[parents];
    parentFields = new List[parents];
    int columns = 0;
    for (int p = 0; p < parents; p++) {
      parentObjInspectors[p] = (StructObjectInspector) inputObjInspectors[p];
      parentFields[p] = parentObjInspectors[p].getAllStructFieldRefs();
      if (p == 0 || parentFields[p].size() < columns) {
        columns = parentFields[p].size();
      }
    }

    // Get columnNames from the first parent
    ArrayList<String> columnNames = new ArrayList<String>(columns);
    for (int c = 0; c < columns; c++) {
      columnNames.add(parentFields[0].get(c).getFieldName());
    }

    // Get outputFieldOIs
    columnTypeResolvers = new ReturnObjectInspectorResolver[columns];
    for (int c = 0; c < columns; c++) {
      columnTypeResolvers[c] = new ReturnObjectInspectorResolver(true);
    }

    for (int p = 0; p < parents; p++) {
      //When columns is 0, the union operator is empty.
      assert (columns == 0 || parentFields[p].size() == columns);
      for (int c = 0; c < columns; c++) {
        if (!columnTypeResolvers[c].updateForUnionAll(parentFields[p].get(c)
            .getFieldObjectInspector())) {
          // checked in SemanticAnalyzer. Should not happen
          throw new HiveException("Incompatible types for union operator");
        }
      }
    }

    ArrayList<ObjectInspector> outputFieldOIs = new ArrayList<ObjectInspector>(
        columns);
    for (int c = 0; c < columns; c++) {
      // can be null for void type
      ObjectInspector fieldOI = parentFields[0].get(c).getFieldObjectInspector();
      outputFieldOIs.add(columnTypeResolvers[c].get(fieldOI));
    }

    // create output row ObjectInspector
    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames, outputFieldOIs);
    outputRow = new ArrayList<Object>(columns);
    for (int c = 0; c < columns; c++) {
      outputRow.add(null);
    }

    // whether we need to do transformation for each parent
    needsTransform = new boolean[parents];
    for (int p = 0; p < parents; p++) {
      // Testing using != is good enough, because we use ObjectInspectorFactory
      // to
      // create ObjectInspectors.
      needsTransform[p] = (inputObjInspectors[p] != outputObjInspector);
      if (needsTransform[p]) {
        LOG.info("Union Operator needs to transform row from parent[" + p
            + "] from " + inputObjInspectors[p] + " to " + outputObjInspector);
      }
    }
  }

  @Override
  public synchronized void process(Object row, int tag) throws HiveException {

    StructObjectInspector soi = parentObjInspectors[tag];
    List<? extends StructField> fields = parentFields[tag];

    if (needsTransform[tag] && outputRow.size() > 0) {
      for (int c = 0; c < fields.size(); c++) {
        outputRow.set(c, columnTypeResolvers[c].convertIfNecessary(soi
            .getStructFieldData(row, fields.get(c)), fields.get(c)
            .getFieldObjectInspector()));
      }
      forward(outputRow, outputObjInspector);
    } else {
      forward(row, inputObjInspectors[tag]);
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return UnionOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "UNION";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.UNION;
  }

  /**
   * Union operators are not allowed either before or after a explicit mapjoin hint.
   * Note that, the same query would just work without the mapjoin hint (by setting
   * hive.auto.convert.join to true).
   **/
  @Override
  public boolean opAllowedBeforeMapJoin() {
    return false;
  }

  @Override
  public boolean opAllowedAfterMapJoin() {
    return false;
  }

  @Override
  public boolean opAllowedBeforeSortMergeJoin() {
    // If a union occurs before the sort-merge join, it is not useful to convert the the
    // sort-merge join to a mapjoin. The number of inputs for the union is more than 1 so
    // it would be difficult to figure out the big table for the mapjoin.
    return false;
  }

  @Override
  public boolean logicalEquals(Operator other) {
    return getClass().getName().equals(other.getClass().getName());
  }
}
