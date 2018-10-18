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

package org.apache.hadoop.hive.ql.plan;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;

import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc.VectorMapOperatorReadType;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorMapDesc.
 *
 * Extra vector information just for the PartitionDesc.
 *
 */
public class VectorPartitionDesc  {

  private static final long serialVersionUID = 1L;

  // Data Type Conversion Needed?
  //
  // VECTORIZED_INPUT_FILE_FORMAT:
  //    No data type conversion check?  Assume ALTER TABLE prevented conversions that
  //    VectorizedInputFileFormat cannot handle...
  //
  // VECTOR_DESERIALIZE:
  //    LAZY_SIMPLE:
  //        Capable of converting on its own.
  //    LAZY_BINARY
  //        Partition schema assumed to match file contents.
  //        Conversion necessary from partition field values to vector columns.
  // ROW_DESERIALIZE
  //    Partition schema assumed to match file contents.
  //    Conversion necessary from partition field values to vector columns.
  //

  public static enum VectorMapOperatorReadType {
    NONE,
    VECTORIZED_INPUT_FILE_FORMAT,
    VECTOR_DESERIALIZE,
    ROW_DESERIALIZE
  }

  public static enum VectorDeserializeType {
    NONE,
    LAZY_SIMPLE,
    LAZY_BINARY
  }

  private VectorMapOperatorReadType vectorMapOperatorReadType;
  private final VectorDeserializeType vectorDeserializeType;

  private final String rowDeserializerClassName;
  private final String inputFileFormatClassName;

  boolean isInputFileFormatSelfDescribing;

  private TypeInfo[] dataTypeInfos;

  private VectorPartitionDesc(String inputFileFormatClassName,
      boolean isInputFileFormatSelfDescribing, VectorMapOperatorReadType vectorMapOperatorReadType) {
    this.vectorMapOperatorReadType = vectorMapOperatorReadType;
    this.vectorDeserializeType = VectorDeserializeType.NONE;
    this.inputFileFormatClassName = inputFileFormatClassName;
    rowDeserializerClassName = null;
    this.isInputFileFormatSelfDescribing = isInputFileFormatSelfDescribing;
    dataTypeInfos = null;
  }

  /**
   * Create a VECTOR_DESERIALIZE flavor object.
   * @param vectorMapOperatorReadType
   * @param vectorDeserializeType
   * @param needsDataTypeConversionCheck
   */
  private VectorPartitionDesc(String inputFileFormatClassName,
      VectorDeserializeType vectorDeserializeType) {
    this.vectorMapOperatorReadType = VectorMapOperatorReadType.VECTOR_DESERIALIZE;
    this.vectorDeserializeType = vectorDeserializeType;
    this.inputFileFormatClassName = inputFileFormatClassName;
    rowDeserializerClassName = null;
    isInputFileFormatSelfDescribing = false;
    dataTypeInfos = null;
  }

  /**
   * Create a ROW_DESERIALIZE flavor object.
   * @param rowDeserializerClassName
   * @param inputFileFormatClassName
   */
  private VectorPartitionDesc(String inputFileFormatClassName,
      boolean isInputFileFormatSelfDescribing, String rowDeserializerClassName) {
    this.vectorMapOperatorReadType = VectorMapOperatorReadType.ROW_DESERIALIZE;
    this.vectorDeserializeType = VectorDeserializeType.NONE;
    this.inputFileFormatClassName = inputFileFormatClassName;
    this.rowDeserializerClassName = rowDeserializerClassName;
    this.isInputFileFormatSelfDescribing = isInputFileFormatSelfDescribing;
    dataTypeInfos = null;
  }

  public static VectorPartitionDesc createVectorizedInputFileFormat(String inputFileFormatClassName,
      boolean isInputFileFormatSelfDescribing) {
    return new VectorPartitionDesc(
        inputFileFormatClassName,
        isInputFileFormatSelfDescribing,
        VectorMapOperatorReadType.VECTORIZED_INPUT_FILE_FORMAT);
  }

  public static VectorPartitionDesc createVectorDeserialize(String inputFileFormatClassName,
      VectorDeserializeType vectorDeserializeType) {
    return new VectorPartitionDesc(inputFileFormatClassName, vectorDeserializeType);
  }

  public static VectorPartitionDesc createRowDeserialize(String inputFileFormatClassName,
      boolean isInputFileFormatSelfDescribing, String rowDeserializerClassName) {
    return new VectorPartitionDesc(rowDeserializerClassName, isInputFileFormatSelfDescribing,
        inputFileFormatClassName);
  }

  @Override
  public VectorPartitionDesc clone() {
    VectorPartitionDesc result;
    switch (vectorMapOperatorReadType) {
    case VECTORIZED_INPUT_FILE_FORMAT:
      result = new VectorPartitionDesc(inputFileFormatClassName, isInputFileFormatSelfDescribing,
          vectorMapOperatorReadType);
      break;
    case VECTOR_DESERIALIZE:
      result = new VectorPartitionDesc(inputFileFormatClassName, vectorDeserializeType);
      break;
    case ROW_DESERIALIZE:
      result = new VectorPartitionDesc(inputFileFormatClassName, isInputFileFormatSelfDescribing,
          rowDeserializerClassName);
      break;
    default:
      throw new RuntimeException("Unexpected vector map operator read type " + vectorMapOperatorReadType.name());
    }
    result.dataTypeInfos = Arrays.copyOf(dataTypeInfos, dataTypeInfos.length);

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof VectorPartitionDesc) {
      VectorPartitionDesc other = (VectorPartitionDesc) o;
      return Strings.nullToEmpty(getInputFileFormatClassName()).equals(
          Strings.nullToEmpty(other.getInputFileFormatClassName())) &&
          Strings.nullToEmpty(getRowDeserializerClassName()).equals(
              Strings.nullToEmpty(other.getRowDeserializerClassName())) &&
          getVectorDeserializeType() == other.getVectorDeserializeType() &&
          getVectorMapOperatorReadType() == other.getVectorMapOperatorReadType() &&
          getIsInputFileFormatSelfDescribing() == other.getIsInputFileFormatSelfDescribing() &&
          Arrays.equals(getDataTypeInfos(), other.getDataTypeInfos());
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = result * prime +
        (getInputFileFormatClassName() == null ? 0 : getInputFileFormatClassName().hashCode());
    result = result * prime +
        (getRowDeserializerClassName() == null ? 0 : getRowDeserializerClassName().hashCode());
    result = result * prime +
        (getVectorDeserializeType() == null ? 0 : getVectorDeserializeType().hashCode());
    result = result * prime +
        (getVectorMapOperatorReadType() == null ? 0 : getVectorMapOperatorReadType().hashCode());
    result = result * prime + Boolean.valueOf(getIsInputFileFormatSelfDescribing()).hashCode();
    result = result * prime + Arrays.hashCode(getDataTypeInfos());
    return result;
  }

  public VectorMapOperatorReadType getVectorMapOperatorReadType() {
    return vectorMapOperatorReadType;
  }

  public String getInputFileFormatClassName() {
    return inputFileFormatClassName;
  }

  public VectorDeserializeType getVectorDeserializeType() {
    return vectorDeserializeType;
  }

  public String getRowDeserializerClassName() {
    return rowDeserializerClassName;
  }

  public boolean getIsInputFileFormatSelfDescribing() {
    return isInputFileFormatSelfDescribing;
  }

  public TypeInfo[] getDataTypeInfos() {
    return dataTypeInfos;
  }

  public void setDataTypeInfos(List<TypeInfo> dataTypeInfoList) {
    dataTypeInfos = dataTypeInfoList.toArray(new TypeInfo[0]);
  }

  public int getDataColumnCount() {
    return dataTypeInfos.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    sb.append(vectorMapOperatorReadType.name());
    sb.append(", ");
    sb.append(inputFileFormatClassName);
    switch (vectorMapOperatorReadType) {
    case VECTORIZED_INPUT_FILE_FORMAT:
      break;
    case VECTOR_DESERIALIZE:
      sb.append(", ");
      sb.append(vectorDeserializeType.name());
      break;
    case ROW_DESERIALIZE:
      sb.append(", ");
      sb.append(rowDeserializerClassName);
      break;
    default:
      throw new RuntimeException("Unexpected vector map operator read type " + vectorMapOperatorReadType.name());
    }
    sb.append(")");
    return sb.toString();
  }

  public void setVectorMapOperatorReadType(VectorMapOperatorReadType val) {
    this.vectorMapOperatorReadType = val;
  }
}
