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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Vectorized extract operator implementation.
 **/
public class VectorExtractOperator extends ExtractOperator implements VectorizationContextRegion {
  private static final long serialVersionUID = 1L;

  private List<TypeInfo> reduceTypeInfos;

  // Create a new outgoing vectorization context because we will project just the values.
  private VectorizationContext vOutContext;

  private int[] projectedColumns;

  private String removeValueDotPrefix(String columnName) {
    return columnName.substring("VALUE.".length());
  }
  public VectorExtractOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    this.conf = (ExtractDesc) conf;

    List<String> reduceColumnNames = vContext.getProjectionColumnNames();
    int reduceColCount = reduceColumnNames.size();

    /*
     * Create a new vectorization context as projection of just the values columns, but 
     * keep same output column manager must be inherited to track the scratch the columns.
     */
    vOutContext = new VectorizationContext(vContext);

    // Set a fileKey with vectorization context.
    vOutContext.setFileKey(vContext.getFileKey() + "/_EXTRACT_");

    // Remove "VALUE." prefix from value columns and create a new projection
    vOutContext.resetProjectionColumns();
    for (int i = 0; i < reduceColCount; i++) {
      String columnName = reduceColumnNames.get(i);
      if (columnName.startsWith("VALUE.")) {
        vOutContext.addProjectionColumn(removeValueDotPrefix(columnName), i);
      }
    }
  }

  public VectorExtractOperator() {
    super();
  }

  /*
   * Called by the Vectorizer class to pass the types from reduce shuffle.
   */
  public void setReduceTypeInfos(List<TypeInfo> reduceTypeInfos) {
    this.reduceTypeInfos = reduceTypeInfos;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    // Create the projection of the values and the output object inspector
    // for just the value without their "VALUE." prefix.
    int projectionSize = vOutContext.getProjectedColumns().size();
    projectedColumns = new int[projectionSize];
    List<String> columnNames = new ArrayList<String>();
    List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    for (int i = 0; i < projectionSize; i++) {
      int projectedIndex = vOutContext.getProjectedColumns().get(i);
      projectedColumns[i] = projectedIndex;
      String colName = vOutContext.getProjectionColumnNames().get(i);
      columnNames.add(colName);
      TypeInfo typeInfo = reduceTypeInfos.get(projectedIndex);
      ObjectInspector oi = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
      ois.add(oi);
    }
    outputObjInspector = ObjectInspectorFactory.
            getStandardStructObjectInspector(columnNames, ois);
    initializeChildren(hconf);
  }

  
  @Override
  // Remove the key columns and forward the values (and scratch columns).
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    int[] originalProjections = vrg.projectedColumns;
    int originalProjectionSize = vrg.projectionSize;

    // Temporarily substitute our projection.
    vrg.projectionSize = projectedColumns.length;
    vrg.projectedColumns = projectedColumns;

    forward(vrg, null);

    // Revert the projected columns back, because vrg will be re-used.
    vrg.projectionSize = originalProjectionSize;
    vrg.projectedColumns = originalProjections;
  }

  @Override
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }
}
