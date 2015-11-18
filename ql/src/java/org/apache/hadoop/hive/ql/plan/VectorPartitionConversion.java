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

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * PartitionConversion.
 *
 */
public class VectorPartitionConversion  {

  private static long serialVersionUID = 1L;

  private boolean validConversion;
  private boolean[] resultConversionFlags;

  private TypeInfo invalidFromTypeInfo;
  private TypeInfo invalidToTypeInfo;

  public boolean getValidConversion() {
    return validConversion;
  }

  public boolean[] getResultConversionFlags() {
    return resultConversionFlags;
  }

  public TypeInfo getInvalidFromTypeInfo() {
    return invalidFromTypeInfo;
  }

  public TypeInfo getInvalidToTypeInfo() {
    return invalidToTypeInfo;
  }

  // Currently, we only support these no-precision-loss or promotion data type conversions:
  //  //
  //  Short -> Int                  IMPLICIT WITH VECTORIZATION
  //  Short -> BigInt               IMPLICIT WITH VECTORIZATION
  //  Int --> BigInt                IMPLICIT WITH VECTORIZATION
  //
  // CONSIDER ADDING:
  //  Float -> Double               IMPLICIT WITH VECTORIZATION
  //  (Char | VarChar) -> String    IMPLICIT WITH VECTORIZATION
  //
  private static HashMap<PrimitiveCategory, PrimitiveCategory[]> validFromPrimitiveMap =
      new HashMap<PrimitiveCategory, PrimitiveCategory[]>();
  static {
    validFromPrimitiveMap.put(
        PrimitiveCategory.SHORT,
        new PrimitiveCategory[] { PrimitiveCategory.INT, PrimitiveCategory.LONG });
    validFromPrimitiveMap.put(
        PrimitiveCategory.INT,
        new PrimitiveCategory[] { PrimitiveCategory.LONG });
  }

  private boolean validateOne(TypeInfo fromTypeInfo, TypeInfo toTypeInfo) {

    if (fromTypeInfo.equals(toTypeInfo)) {
      return false;
    }

    if (fromTypeInfo.getCategory() == Category.PRIMITIVE &&
        toTypeInfo.getCategory() == Category.PRIMITIVE) {

      PrimitiveCategory fromPrimitiveCategory = ((PrimitiveTypeInfo) fromTypeInfo).getPrimitiveCategory();
      PrimitiveCategory toPrimitiveCategory = ((PrimitiveTypeInfo) toTypeInfo).getPrimitiveCategory();

      PrimitiveCategory[] toPrimitiveCategories =
          validFromPrimitiveMap.get(fromPrimitiveCategory);
      if (toPrimitiveCategories == null ||
          !ArrayUtils.contains(toPrimitiveCategories, toPrimitiveCategory)) {
        invalidFromTypeInfo = fromTypeInfo;
        invalidToTypeInfo = toTypeInfo;

        // Tell caller a bad one was found.
        validConversion = false;
        return false;
      }
    } else {
      // Ignore checking complex types.  Assume they will not be included in the query.
    }

    return true;
  }

  public void validateConversion(List<TypeInfo> fromTypeInfoList,
      List<TypeInfo> toTypeInfoList) {

    final int columnCount = fromTypeInfoList.size();
    resultConversionFlags = new boolean[columnCount];

    // The method validateOne will turn this off when invalid conversion is found.
    validConversion = true;

    boolean atLeastOneConversion = false;
    for (int i = 0; i < columnCount; i++) {
      TypeInfo fromTypeInfo = fromTypeInfoList.get(i);
      TypeInfo toTypeInfo = toTypeInfoList.get(i);

      resultConversionFlags[i] = validateOne(fromTypeInfo, toTypeInfo);
      if (!validConversion) {
        return;
      }
    }

    if (atLeastOneConversion) {
      // Leave resultConversionFlags set.
    } else {
      resultConversionFlags = null;
    }
  }

  public void validateConversion(TypeInfo[] fromTypeInfos, TypeInfo[] toTypeInfos) {

    final int columnCount = fromTypeInfos.length;
    resultConversionFlags = new boolean[columnCount];

    // The method validateOne will turn this off when invalid conversion is found.
    validConversion = true;

    boolean atLeastOneConversion = false;
    for (int i = 0; i < columnCount; i++) {
      TypeInfo fromTypeInfo = fromTypeInfos[i];
      TypeInfo toTypeInfo = toTypeInfos[i];

      resultConversionFlags[i] = validateOne(fromTypeInfo, toTypeInfo);
      if (!validConversion) {
        return;
      }
      if (resultConversionFlags[i]) {
        atLeastOneConversion = true;
      }
    }

    if (atLeastOneConversion) {
      // Leave resultConversionFlags set.
    } else {
      resultConversionFlags = null;
    }
  }
}