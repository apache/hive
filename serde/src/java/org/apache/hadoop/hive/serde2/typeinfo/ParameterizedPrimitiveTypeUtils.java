package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

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
public class ParameterizedPrimitiveTypeUtils {

  public static BaseTypeParams getTypeParamsFromTypeInfo(TypeInfo typeInfo) {
    BaseTypeParams typeParams = null;
    if (typeInfo instanceof PrimitiveTypeInfo) {
      PrimitiveTypeInfo ppti = (PrimitiveTypeInfo)typeInfo;
      typeParams = ppti.getTypeParams();
    }
    return typeParams;
  }

  public static BaseTypeParams getTypeParamsFromPrimitiveTypeEntry(PrimitiveTypeEntry typeEntry) {
    return typeEntry.typeParams;
  }

  public static BaseTypeParams getTypeParamsFromPrimitiveObjectInspector(
      PrimitiveObjectInspector oi) {
    return oi.getTypeParams();
  }

  /**
   * Utils for varchar type
   */
  public static class HiveVarcharSerDeHelper {
    public int maxLength;
    public HiveVarcharWritable writable = new HiveVarcharWritable();

    public HiveVarcharSerDeHelper(VarcharTypeParams typeParams) {
      if (typeParams == null) {
        throw new RuntimeException("varchar type used without type params");
      }
      maxLength = typeParams.getLength();
    }
  }

  public static boolean doesWritableMatchTypeParams(HiveVarcharWritable writable,
      VarcharTypeParams typeParams) {
    return (typeParams == null || typeParams.length >= writable.getCharacterLength());
  }

  public static boolean doesPrimitiveMatchTypeParams(HiveVarchar value,
      VarcharTypeParams typeParams) {
    return (typeParams == null || typeParams.length == value.getCharacterLength());
  }
}
