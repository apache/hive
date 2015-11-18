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

package org.apache.hadoop.hive.ql.plan;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorMapDesc.
 *
 * Extra vector information just for the PartitionDesc.
 *
 */
public class VectorPartitionDesc  {

  private static long serialVersionUID = 1L;

  // Data Type Conversion Needed?
  //
  // VECTORIZED_INPUT_FILE_FORMAT:
  //    No data type conversion check?  Assume ALTER TABLE prevented conversions that
  //    VectorizedInputFileFormat cannot handle...
  //

  public static enum VectorMapOperatorReadType {
    NONE,
    VECTORIZED_INPUT_FILE_FORMAT
  }


  private final VectorMapOperatorReadType vectorMapOperatorReadType;

  private final boolean needsDataTypeConversionCheck;

  private boolean[] conversionFlags;

  private TypeInfo[] typeInfos;

  private VectorPartitionDesc(VectorMapOperatorReadType vectorMapOperatorReadType,
      boolean needsDataTypeConversionCheck) {
    this.vectorMapOperatorReadType = vectorMapOperatorReadType;
    this.needsDataTypeConversionCheck = needsDataTypeConversionCheck;

    conversionFlags = null;
    typeInfos = null;
  }

  public static VectorPartitionDesc createVectorizedInputFileFormat() {
    return new VectorPartitionDesc(VectorMapOperatorReadType.VECTORIZED_INPUT_FILE_FORMAT, true);
  }


  @Override
  public VectorPartitionDesc clone() {
    VectorPartitionDesc result =
        new VectorPartitionDesc(vectorMapOperatorReadType,
            needsDataTypeConversionCheck);
    result.conversionFlags =
        (conversionFlags == null ? null :
          Arrays.copyOf(conversionFlags, conversionFlags.length));
    result.typeInfos = Arrays.copyOf(typeInfos, typeInfos.length);
    return result;
  }

  public VectorMapOperatorReadType getVectorMapOperatorReadType() {
    return vectorMapOperatorReadType;
  }

  public boolean getNeedsDataTypeConversionCheck() {
    return needsDataTypeConversionCheck;
  }

  public void setConversionFlags(boolean[] conversionFlags) {
    this.conversionFlags = conversionFlags;
  }

  public boolean[] getConversionFlags() {
    return conversionFlags;
  }

  public TypeInfo[] getTypeInfos() {
    return typeInfos;
  }

  public void setTypeInfos(List<TypeInfo> typeInfoList) {
    typeInfos = typeInfoList.toArray(new TypeInfo[0]);
  }

  public int getNonPartColumnCount() {
    return typeInfos.length;
  }
}
