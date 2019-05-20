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

package org.apache.hadoop.hive.ql.exec.vector.udf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Descriptor for function argument.
 */
public class VectorUDFArgDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean isConstant;
  private int columnNum;
  private transient GenericUDF.DeferredJavaObject constObjVal;
  private ExprNodeConstantDesc constExpr;

  public VectorUDFArgDesc() {
  }

  /**
   * Set this argument to a constant value extracted from the
   * expression tree.
   */
  public void setConstant(ExprNodeConstantDesc expr) {
    isConstant = true;
    if (expr != null) {
      if (expr.getTypeInfo().getCategory() == Category.PRIMITIVE) {
        PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) expr.getTypeInfo())
            .getPrimitiveCategory();
        if (primitiveCategory == PrimitiveCategory.VOID) {
          // Otherwise we'd create a NullWritable and that isn't what we want.
          expr = null;
        }
      }
    }

    constExpr = expr;
  }

  /* Prepare the constant for use when the function is called. To be used
   * during initialization.
   */
  public void prepareConstant() {
    final Object writableValue;
    if (constExpr != null) {
      Object constantValue = constExpr.getValue();
      TypeInfo typeInfo = constExpr.getTypeInfo();
      ObjectInspector objectInspector =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
      Category category = typeInfo.getCategory();
      switch (category) {
      case PRIMITIVE:
        {
          PrimitiveCategory pc =
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();

          // Convert from Java to Writable
          AbstractPrimitiveJavaObjectInspector primitiveJavaObjectInspector =
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pc);
          writableValue =
              primitiveJavaObjectInspector.getPrimitiveWritableObject(constantValue);
        }
        break;
      case STRUCT:
        {
          if (constantValue.getClass().isArray()) {
            constantValue = java.util.Arrays.asList((Object[]) constantValue);
          }

          StructObjectInspector structObjectInspector =
              (StructObjectInspector) objectInspector;
          List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
          List<String> fieldNames = new ArrayList<String>(fields.size());
          List<ObjectInspector> fieldObjectInspectors =
              new ArrayList<ObjectInspector>(fields.size());
          for (StructField f : fields) {
            fieldNames.add(f.getFieldName());
            fieldObjectInspectors.add(
                ObjectInspectorUtils.getStandardObjectInspector(
                    f.getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE));
          }

          StandardConstantStructObjectInspector constantStructObjectInspector =
              ObjectInspectorFactory.getStandardConstantStructObjectInspector(
              fieldNames,
              fieldObjectInspectors,
              (List<?>) constantValue);
          writableValue =
              constantStructObjectInspector.getWritableConstantValue();
        }
        break;
      default:
        throw new RuntimeException("Unexpected category " + category);
      }
    } else {
      writableValue = null;
    }

    constObjVal = new GenericUDF.DeferredJavaObject(writableValue);
  }

  /**
   * Set this argument to be a "variable" one which is to be taken from
   * a specified column vector number i.
   */
  public void setVariable(int i) {
    columnNum = i;
  }

  public boolean isConstant() {
    return isConstant;
  }

  public boolean isVariable() {
    return !isConstant;
  }

  public int getColumn() {
    return columnNum;
  }

  public DeferredObject getDeferredJavaObject(int row, VectorizedRowBatch b, int argPosition,
      VectorExpressionWriter[] writers) {

    if (isConstant()) {
      return this.constObjVal;
    } else {

      // get column
      ColumnVector cv = b.cols[columnNum];

      // write value to object that can be inspected
      Object o;
      try {
        o = writers[argPosition].writeValue(cv, row);
        return new GenericUDF.DeferredJavaObject(o);
      } catch (HiveException e) {
        throw new RuntimeException("Unable to get Java object from VectorizedRowBatch", e);
      }
    }
  }

  public boolean getIsConstant() {
    return isConstant;
  }

  public void setIsConstant(boolean isConstant) {
    this.isConstant = isConstant;
  }

  public int getColumnNum() {
    return columnNum;
  }

  public void setColumnNum(int columnNum) {
    this.columnNum = columnNum;
  }

  public ExprNodeConstantDesc getConstExpr() {
    return constExpr;
  }

  public void setConstExpr(ExprNodeConstantDesc constExpr) {
    this.constExpr = constExpr;
  }
}
