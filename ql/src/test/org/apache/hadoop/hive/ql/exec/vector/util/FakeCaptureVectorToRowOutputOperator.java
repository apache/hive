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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Operator that captures output emitted by parent.
 * Used in unit test only.
 */
public class FakeCaptureVectorToRowOutputOperator extends FakeCaptureOutputOperator
  implements Serializable {
  private static final long serialVersionUID = 1L;

  private Operator<? extends OperatorDesc> op;

  private TypeInfo[] outputTypeInfos;
  private ObjectInspector[] outputObjectInspectors;
  private VectorExtractRow vectorExtractRow;

  /** Kryo ctor. */
  protected FakeCaptureVectorToRowOutputOperator() {
    super();
  }

  public FakeCaptureVectorToRowOutputOperator(CompilationOpContext ctx,
      Operator<? extends OperatorDesc> op) {
    super(ctx);
    this.op = op;
  }

  public static FakeCaptureVectorToRowOutputOperator addCaptureOutputChild(CompilationOpContext ctx,
      Operator<? extends OperatorDesc> op) {
    FakeCaptureVectorToRowOutputOperator out = new FakeCaptureVectorToRowOutputOperator(ctx, op);
    List<Operator<? extends OperatorDesc>> listParents =
        new ArrayList<Operator<? extends OperatorDesc>>(1);
    listParents.add(op);
    out.setParentOperators(listParents);
    List<Operator<? extends OperatorDesc>> listChildren =
        new ArrayList<Operator<? extends OperatorDesc>>(1);
    listChildren.add(out);
    op.setChildOperators(listChildren);
    return out;
  }


  @Override
  public void initializeOp(Configuration conf) throws HiveException {
    super.initializeOp(conf);

    VectorizationContextRegion vectorizationContextRegion = (VectorizationContextRegion) op;
    VectorizationContext outputVectorizationContext =
        vectorizationContextRegion.getOutputVectorizationContext();
    outputTypeInfos = outputVectorizationContext.getInitialTypeInfos();

    final int outputLength = outputTypeInfos.length;
    outputObjectInspectors = new ObjectInspector[outputLength];
    for (int i = 0; i < outputLength; i++) {
      TypeInfo typeInfo = outputTypeInfos[i];
      outputObjectInspectors[i] =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
    }
    vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(outputTypeInfos);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logical = 0; logical < batch.size; logical++) {
      int batchIndex = (selectedInUse ? selected[logical] : logical);
      Object[] rowObjects = new Object[outputObjectInspectors.length];
      vectorExtractRow.extractRow(batch, batchIndex, rowObjects);
      for (int c = 0; c < rowObjects.length; c++) {
        switch (outputTypeInfos[c].getCategory()) {
        case PRIMITIVE:
          rowObjects[c] =
              ((PrimitiveObjectInspector) outputObjectInspectors[c]).copyObject(
                  rowObjects[c]);
          break;
        case STRUCT:
          {
            final StructTypeInfo structTypeInfo = (StructTypeInfo) outputTypeInfos[c];
            final StandardStructObjectInspector structInspector =
                (StandardStructObjectInspector) outputObjectInspectors[c];
            final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
            final int size = fieldTypeInfos.size();
            final List<? extends StructField> structFields =
                structInspector.getAllStructFieldRefs();

            final Object oldStruct = rowObjects[c];
            if (oldStruct != null) {
              List<Object> currentStructData =
                  structInspector.getStructFieldsDataAsList(oldStruct);
              final Object newStruct = structInspector.create();
              for (int i = 0; i < size; i++) {
                final StructField structField = structFields.get(i);
                final Object oldValue = currentStructData.get(i);
                final Object newValue;
                if (oldValue != null) {
                  newValue =
                      ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).copyObject(
                          oldValue);
                } else {
                  newValue = null;
                }
                structInspector.setStructFieldData(newStruct, structField, newValue);
              }
              rowObjects[c] = ((ArrayList<Object>) newStruct).toArray();
            }
          }
          break;
        default:
          throw new RuntimeException("Unexpected category " + outputTypeInfos[c].getCategory());
        }
      }
      super.process(rowObjects, 0);
    }
  }
}