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

import java.util.HashMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * PartitionConversion.
 *
 */
public class VectorPartitionConversion  {

  // Currently, we only support these no-precision-loss or promotion data type conversions:
  //
  //  TinyInt --> SmallInt
  //  TinyInt --> Int
  //  TinyInt --> BigInt
  //
  //  SmallInt -> Int
  //  SmallInt -> BigInt
  //
  //  Int --> BigInt
  //
  //  Float -> Double
  //
  //  Since we stare Char without padding, it can become a String implicitly.
  //  (Char | VarChar) -> String
  //
  private static HashMap<PrimitiveCategory, PrimitiveCategory[]> implicitPrimitiveMap =
      new HashMap<PrimitiveCategory, PrimitiveCategory[]>();
  static {
    implicitPrimitiveMap.put(
        PrimitiveCategory.BOOLEAN,
        new PrimitiveCategory[] {
            PrimitiveCategory.BYTE, PrimitiveCategory.SHORT, PrimitiveCategory.INT, PrimitiveCategory.LONG });
    implicitPrimitiveMap.put(
        PrimitiveCategory.BYTE,
        new PrimitiveCategory[] {
            PrimitiveCategory.SHORT, PrimitiveCategory.INT, PrimitiveCategory.LONG });
    implicitPrimitiveMap.put(
        PrimitiveCategory.SHORT,
        new PrimitiveCategory[] {
            PrimitiveCategory.INT, PrimitiveCategory.LONG });
    implicitPrimitiveMap.put(
        PrimitiveCategory.INT,
        new PrimitiveCategory[] {
            PrimitiveCategory.LONG });
    implicitPrimitiveMap.put(
        PrimitiveCategory.FLOAT,
        new PrimitiveCategory[] {
            PrimitiveCategory.DOUBLE });
    implicitPrimitiveMap.put(
        PrimitiveCategory.CHAR,
        new PrimitiveCategory[] {
            PrimitiveCategory.STRING });
    implicitPrimitiveMap.put(
        PrimitiveCategory.VARCHAR,
        new PrimitiveCategory[] {
            PrimitiveCategory.STRING });
  }

  public static boolean isImplicitVectorColumnConversion(TypeInfo fromTypeInfo,
      TypeInfo toTypeInfo) {

    if (fromTypeInfo.getCategory() == Category.PRIMITIVE &&
        toTypeInfo.getCategory() == Category.PRIMITIVE) {

      PrimitiveCategory fromPrimitiveCategory =
          ((PrimitiveTypeInfo) fromTypeInfo).getPrimitiveCategory();
      PrimitiveCategory toPrimitiveCategory =
          ((PrimitiveTypeInfo) toTypeInfo).getPrimitiveCategory();
      PrimitiveCategory[] toPrimitiveCategories = implicitPrimitiveMap.get(fromPrimitiveCategory);
      if (toPrimitiveCategories != null) {
        for (PrimitiveCategory candidatePrimitiveCategory : toPrimitiveCategories) {
          if (candidatePrimitiveCategory == toPrimitiveCategory) {
            return true;
          }
        }
      }
      return false;
    }
    return false;
  }
}