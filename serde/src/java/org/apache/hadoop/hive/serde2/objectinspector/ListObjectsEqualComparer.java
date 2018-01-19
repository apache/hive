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

package org.apache.hadoop.hive.serde2.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Compare two list of objects.
 * Two lists are expected to have same types. Type information for every object is
 * passed when calling Constructor to avoid the step of figuring out types from
 * ObjectInspetor and determine how to compare the types when comparing.
 * Also, for string and text elements, it performs slightly better than
 * using ObjectInspectorUtils.compare() == 0, which instead of calling .compare()
 * calls .equalTo(), which compares size before byte by byte comparison.
 *
 */
public class ListObjectsEqualComparer {
  enum CompareType {
    // Now only string, text, int, long, byte and boolean comparisons
    // are treated as special cases.
    // For other types, we reuse ObjectInspectorUtils.compare()
    COMPARE_STRING, COMPARE_TEXT, COMPARE_INT, COMPARE_LONG, COMPARE_BYTE,
    COMPARE_BOOL, OTHER
  }

  class FieldComparer {
    protected ObjectInspector oi0, oi1;
    protected ObjectInspector compareOI;
    protected CompareType compareType;
    protected Converter converter0, converter1;
    protected StringObjectInspector soi0, soi1;
    protected IntObjectInspector ioi0, ioi1;
    protected LongObjectInspector loi0, loi1;
    protected ByteObjectInspector byoi0, byoi1;
    protected BooleanObjectInspector boi0, boi1;

    public FieldComparer(ObjectInspector oi0, ObjectInspector oi1) {
      this.oi0 = oi0;
      this.oi1 = oi1;

      TypeInfo type0 = TypeInfoUtils.getTypeInfoFromObjectInspector(oi0);
      TypeInfo type1 = TypeInfoUtils.getTypeInfoFromObjectInspector(oi1);
      if (type0.equals(TypeInfoFactory.stringTypeInfo) &&
          type1.equals(TypeInfoFactory.stringTypeInfo)) {
        soi0 = (StringObjectInspector) oi0;
        soi1 = (StringObjectInspector) oi1;
        if (soi0.preferWritable() || soi1.preferWritable()) {
          compareType = CompareType.COMPARE_TEXT;
        } else {
          compareType = CompareType.COMPARE_STRING;
        }
      } else if (type0.equals(TypeInfoFactory.intTypeInfo) &&
          type1.equals(TypeInfoFactory.intTypeInfo)) {
        compareType = CompareType.COMPARE_INT;
        ioi0 = (IntObjectInspector) oi0;
        ioi1 = (IntObjectInspector) oi1;
      } else if (type0.equals(TypeInfoFactory.longTypeInfo) &&
          type1.equals(TypeInfoFactory.longTypeInfo)) {
        compareType = CompareType.COMPARE_LONG;
        loi0 = (LongObjectInspector) oi0;
        loi1 = (LongObjectInspector) oi1;
      } else if (type0.equals(TypeInfoFactory.byteTypeInfo) &&
          type1.equals(TypeInfoFactory.byteTypeInfo)) {
        compareType = CompareType.COMPARE_BYTE;
        byoi0 = (ByteObjectInspector) oi0;
        byoi1 = (ByteObjectInspector) oi1;
      } else if (type0.equals(TypeInfoFactory.booleanTypeInfo) &&
          type1.equals(TypeInfoFactory.booleanTypeInfo)) {
        compareType = CompareType.COMPARE_BOOL;
        boi0 = (BooleanObjectInspector) oi0;
        boi1 = (BooleanObjectInspector) oi1;
      } else {
        // We don't check compatibility of two object inspectors, but directly
        // pass them into ObjectInspectorUtils.compare(), users of this class
        // should make sure ObjectInspectorUtils.compare() doesn't throw exceptions
        // and returns correct results.
        compareType = CompareType.OTHER;
      }
    }

    public boolean areEqual(Object o0, Object o1) {
      if (o0 == null && o1 == null) {
        return true;
      } else if (o0 == null || o1 == null) {
        return false;
      }
      switch (compareType) {
      case COMPARE_TEXT:
        return (soi0.getPrimitiveWritableObject(o0).equals(
            soi1.getPrimitiveWritableObject(o1)));
      case COMPARE_INT:
        return (ioi0.get(o0) == ioi1.get(o1));
      case COMPARE_LONG:
        return (loi0.get(o0) == loi1.get(o1));
      case COMPARE_BYTE:
        return (byoi0.get(o0) == byoi1.get(o1));
      case COMPARE_BOOL:
        return (boi0.get(o0) == boi1.get(o1));
      case COMPARE_STRING:
        return (soi0.getPrimitiveJavaObject(o0).equals(
            soi1.getPrimitiveJavaObject(o1)));
      default:
        return (ObjectInspectorUtils.compare(
            o0, oi0, o1, oi1) == 0);
      }
    }
  }

  FieldComparer[] fieldComparers;
  int numFields;

  public ListObjectsEqualComparer(ObjectInspector[] oi0, ObjectInspector[] oi1) {
    if (oi0.length != oi1.length) {
      throw new RuntimeException("Sizes of two lists of object inspectors don't match.");
    }
    numFields = oi0.length;
    fieldComparers = new FieldComparer[numFields];
    for (int i = 0; i < oi0.length; i++) {
      fieldComparers[i] = new FieldComparer(oi0[i], oi1[i]);
    }
  }


  /**
   * ol0, ol1 should have equal or less number of elements than objectinspectors
   * passed in constructor.
   *
   * @param ol0
   * @param ol1
   * @return True if object in ol0 and ol1 are all identical
   */
  public boolean areEqual(Object[] ol0, Object[] ol1) {
    if (ol0.length != numFields || ol1.length != numFields) {
      if (ol0.length != ol1.length) {
        return false;
      }
      assert (ol0.length <= numFields);
      assert (ol1.length <= numFields);
      for (int i = 0; i < Math.min(ol0.length, ol1.length); i++) {
        if (!fieldComparers[i].areEqual(ol0[i], ol1[i])) {
          return false;
        }
      }
      return true;
    }

    for (int i = numFields - 1; i >= 0; i--) {
      if (!fieldComparers[i].areEqual(ol0[i], ol1[i])) {
        return false;
      }
    }
    return true;
  }
}
