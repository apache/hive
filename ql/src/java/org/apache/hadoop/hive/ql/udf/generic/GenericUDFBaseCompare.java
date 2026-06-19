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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDF Base Class for operations.
 */
@Description(name = "op", value = "a op b - Returns the result of operation")
public abstract class GenericUDFBaseCompare extends GenericUDFBaseBinary {
  public enum CompareType {
    // Now only string, text, int, long, byte and boolean comparisons are
    // treated as special cases.
    // For other types, we reuse ObjectInspectorUtils.compare()
    COMPARE_STRING, COMPARE_TEXT, COMPARE_INT, COMPARE_LONG, COMPARE_BYTE,
    COMPARE_BOOL, SAME_TYPE, NEED_CONVERT
  }

  protected transient ObjectInspector[] argumentOIs;

  protected transient ReturnObjectInspectorResolver conversionHelper = null;
  protected ObjectInspector compareOI;
  protected CompareType compareType;
  protected transient Converter converter0, converter1;
  protected transient StringObjectInspector soi0, soi1;
  protected transient IntObjectInspector ioi0, ioi1;
  protected transient LongObjectInspector loi0, loi1;
  protected transient ByteObjectInspector byoi0, byoi1;
  protected transient BooleanObjectInspector boi0,boi1;
  protected final BooleanWritable result = new BooleanWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException(
          opName + " requires two arguments.");
    }

    argumentOIs = arguments;

    Category c1 = arguments[0].getCategory();
    Category c2 = arguments[1].getCategory();
    if (c1 != c2) {
      throw new UDFArgumentException("Type mismatch in " + opName + "(" + c1 + "," + c2 + ")");
    }
    if (!supportsCategory(c1)) {
      throw new UDFArgumentException(opName + " does not support " + c1 + " types");
    }

    switch (c1) {
    case PRIMITIVE:
      initForPrimitives(arguments[0], arguments[1]);
      break;
    case MAP:
    case STRUCT:
    case LIST:
      initForNonPrimitives(arguments[0], arguments[1]);
      break;
    default:
      throw new AssertionError("Missing init method for " + c1 + " types");
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

  }

  /**
   * Returns whether the operator can handle operands with the specified category.
   */
  protected boolean supportsCategory(Category c){
    return c == Category.PRIMITIVE;
  }

  private void initForPrimitives(ObjectInspector arg0, ObjectInspector arg1) throws UDFArgumentException {
    assert arg0.getCategory() == Category.PRIMITIVE;
    assert arg1.getCategory() == Category.PRIMITIVE;
    final TypeInfo type0 = TypeInfoUtils.getTypeInfoFromObjectInspector(arg0);
    final TypeInfo type1 = TypeInfoUtils.getTypeInfoFromObjectInspector(arg1);
    if (type0.equals(TypeInfoFactory.stringTypeInfo) && type1.equals(TypeInfoFactory.stringTypeInfo)) {
      soi0 = (StringObjectInspector) arg0;
      soi1 = (StringObjectInspector) arg1;
      if (soi0.preferWritable() || soi1.preferWritable()) {
        compareType = CompareType.COMPARE_TEXT;
      } else {
        compareType = CompareType.COMPARE_STRING;
      }
    } else if (type0.equals(TypeInfoFactory.intTypeInfo) && type1.equals(TypeInfoFactory.intTypeInfo)) {
      compareType = CompareType.COMPARE_INT;
      ioi0 = (IntObjectInspector) arg0;
      ioi1 = (IntObjectInspector) arg1;
    } else if (type0.equals(TypeInfoFactory.longTypeInfo) && type1.equals(TypeInfoFactory.longTypeInfo)) {
      compareType = CompareType.COMPARE_LONG;
      loi0 = (LongObjectInspector) arg0;
      loi1 = (LongObjectInspector) arg1;
    } else if (type0.equals(TypeInfoFactory.byteTypeInfo) && type1.equals(TypeInfoFactory.byteTypeInfo)) {
      compareType = CompareType.COMPARE_BYTE;
      byoi0 = (ByteObjectInspector) arg0;
      byoi1 = (ByteObjectInspector) arg1;
    } else if (type0.equals(TypeInfoFactory.booleanTypeInfo) && type1.equals(TypeInfoFactory.booleanTypeInfo)) {
      compareType = CompareType.COMPARE_BOOL;
      boi0 = (BooleanObjectInspector) arg0;
      boi1 = (BooleanObjectInspector) arg1;
    } else {
      if (type0 == type1 || TypeInfoUtils.doPrimitiveCategoriesMatch(type0, type1)) {
        compareType = CompareType.SAME_TYPE;
      } else {
        compareType = CompareType.NEED_CONVERT;
        TypeInfo compareType = FunctionRegistry.getCommonClassForComparison(type0, type1);

        // For now, we always convert to double if we can't find a common type
        compareOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            (compareType == null) ? TypeInfoFactory.doubleTypeInfo : compareType);

        converter0 = ObjectInspectorConverters.getConverter(arg0, compareOI);
        converter1 = ObjectInspectorConverters.getConverter(arg1, compareOI);

        checkConversionAllowed(arg0, compareOI);
        checkConversionAllowed(arg1, compareOI);
      }
    }
  }

  private void initForNonPrimitives(ObjectInspector arg0, ObjectInspector arg1) throws UDFArgumentException {
    assert arg0.getCategory() != Category.PRIMITIVE;
    assert arg1.getCategory() != Category.PRIMITIVE;
    assert arg0.getCategory() == arg1.getCategory();
    final TypeInfo type0 = TypeInfoUtils.getTypeInfoFromObjectInspector(arg0);
    final TypeInfo type1 = TypeInfoUtils.getTypeInfoFromObjectInspector(arg1);
    if (type0.equals(type1)) {
      compareType = CompareType.SAME_TYPE;
    } else {
      throw new UDFArgumentException("Type mismatch in " + opName + "(" + type0 + "," + type1 + ")");
    }
  }
  
  protected void checkConversionAllowed(ObjectInspector argOI, ObjectInspector compareOI)
      throws UDFArgumentException {
    if (primitiveGroupOf(argOI) != PrimitiveGrouping.DATE_GROUP) {
      return;
    }
    SessionState ss = SessionState.get();
    if (ss != null && ss.getConf().getBoolVar(ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION)) {
      if (primitiveGroupOf(compareOI) == PrimitiveGrouping.NUMERIC_GROUP) {
        throw new UDFArgumentException(
            "Casting DATE/TIMESTAMP to NUMERIC is prohibited (" + ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION + ")");
      }
    }
  }

  protected PrimitiveGrouping primitiveGroupOf(ObjectInspector oi) {
    if (oi instanceof PrimitiveObjectInspector) {
      PrimitiveCategory category = ((PrimitiveObjectInspector) oi).getPrimitiveCategory();
      PrimitiveGrouping group = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(category);
      return group;
    } else {
      return null;
    }
  }

  public Integer compare(DeferredObject[] arguments) throws HiveException {
    Object o0,o1;
    o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    o1 = arguments[1].get();
    if (o1 == null) {
      return null;
    }

    if (compareType == CompareType.NEED_CONVERT) {
      Object converted_o0 = converter0.convert(o0);
      if (converted_o0 == null) {
        return null;
      }
      Object converted_o1 = converter1.convert(o1);
      if (converted_o1 == null) {
        return null;
      }
      return ObjectInspectorUtils.compare(
          converted_o0, compareOI,
          converted_o1, compareOI);
    } else {
      return ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]);
    }
  }
}
