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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDF Base Class for operations.
 */
@Description(name = "op", value = "a op b - Returns the result of operation")
public abstract class GenericUDFBaseCompare extends GenericUDF {
  public enum CompareType {
    // Now only string, int and bollean comparisons are treated as special cases.
    // For other types, we reuse ObjectInspectorUtils.compare()
    COMPARE_STRING, COMPARE_TEXT, COMPARE_INT, COMPARE_LONG, COMPARE_BYTE,
    COMPARE_BOOL, SAME_TYPE, NEED_CONVERT
  }

  protected String opName;
  protected String opDisplayName;

  protected ObjectInspector[] argumentOIs;

  protected ReturnObjectInspectorResolver conversionHelper = null;
  protected ObjectInspector compareOI;
  protected CompareType compareType;
  protected Converter converter0, converter1;
  protected StringObjectInspector soi0, soi1;
  protected IntObjectInspector ioi0, ioi1;
  protected LongObjectInspector loi0, loi1;
  protected ByteObjectInspector byoi0, byoi1;
  protected BooleanObjectInspector boi0,boi1;
  protected final BooleanWritable result = new BooleanWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException(
          opName + " requires two arguments.");
    }

    argumentOIs = arguments;

    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of " + opName + "  is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]) ==
      TypeInfoFactory.stringTypeInfo &&
      TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]) ==
        TypeInfoFactory.stringTypeInfo) {
      soi0 = (StringObjectInspector) arguments[0];
      soi1 = (StringObjectInspector) arguments[1];
      if (soi0.preferWritable() || soi1.preferWritable()) {
        compareType = CompareType.COMPARE_TEXT;
      } else {
        compareType = CompareType.COMPARE_STRING;
      }
    } else if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
      TypeInfoFactory.intTypeInfo) &&
      TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
        TypeInfoFactory.intTypeInfo)) {
      compareType = CompareType.COMPARE_INT;
      ioi0 = (IntObjectInspector) arguments[0];
      ioi1 = (IntObjectInspector) arguments[1];
    } else if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
        TypeInfoFactory.longTypeInfo) &&
        TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
          TypeInfoFactory.longTypeInfo)) {
        compareType = CompareType.COMPARE_LONG;
        loi0 = (LongObjectInspector) arguments[0];
        loi1 = (LongObjectInspector) arguments[1];
    } else if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
        TypeInfoFactory.byteTypeInfo) &&
        TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
          TypeInfoFactory.byteTypeInfo)) {
        compareType = CompareType.COMPARE_BYTE;
        byoi0 = (ByteObjectInspector) arguments[0];
        byoi1 = (ByteObjectInspector) arguments[1];
    } else if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
        TypeInfoFactory.booleanTypeInfo) &&
        TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
          TypeInfoFactory.booleanTypeInfo)) {
      compareType = CompareType.COMPARE_BOOL;
      boi0 = (BooleanObjectInspector) arguments[0];
      boi1 = (BooleanObjectInspector) arguments[1];
     } else {
      TypeInfo oiTypeInfo0 = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]);
      TypeInfo oiTypeInfo1 = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]);

      if (oiTypeInfo0 != oiTypeInfo1) {
        compareType = CompareType.NEED_CONVERT;
        compareOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            TypeInfoFactory.doubleTypeInfo);
        converter0 = ObjectInspectorConverters.getConverter(arguments[0], compareOI);
        converter1 = ObjectInspectorConverters.getConverter(arguments[1], compareOI);
      } else {
        compareType = CompareType.SAME_TYPE;
      }
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

  }


  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "(" + children[0] + " " + opDisplayName + " " + children[1] + ")";

  }

}
