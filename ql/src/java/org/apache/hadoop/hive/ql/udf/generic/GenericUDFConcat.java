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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringGroupConcatColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringGroupColConcatStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringGroupColConcatCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringGroupColConcatVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringScalarConcatStringGroupCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CharScalarConcatStringGroupCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VarCharScalarConcatStringGroupCol;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.StringConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * GenericUDFConcat.
 */
@Description(name = "concat",
value = "_FUNC_(str1, str2, ... strN) - returns the concatenation of str1, str2, ... strN or "+
        "_FUNC_(bin1, bin2, ... binN) - returns the concatenation of bytes in binary data " +
        " bin1, bin2, ... binN",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > SELECT _FUNC_('abc', 'def') FROM src LIMIT 1;\n"
+ "  'abcdef'")
@VectorizedExpressions({StringGroupConcatColCol.class,
    StringGroupColConcatStringScalar.class,
    StringGroupColConcatCharScalar.class, StringGroupColConcatVarCharScalar.class,
    StringScalarConcatStringGroupCol.class,
    CharScalarConcatStringGroupCol.class, VarCharScalarConcatStringGroupCol.class})
public class GenericUDFConcat extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;
  private transient StringConverter[] stringConverters;
  private transient PrimitiveCategory returnType = PrimitiveCategory.STRING;
  private transient BytesWritable[] bw;
  private transient GenericUDFUtils.StringHelper returnHelper;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    // Loop through all the inputs to determine the appropriate return type/length.
    // Return type:
    //  All CHAR inputs: return CHAR
    //  All VARCHAR inputs: return VARCHAR
    //  All CHAR/VARCHAR inputs: return VARCHAR
    //  All BINARY inputs: return BINARY
    //  Otherwise return STRING
    argumentOIs = arguments;

    PrimitiveCategory currentCategory;
    PrimitiveObjectInspector poi;
    boolean fixedLengthReturnValue = true;
    int returnLength = 0;  // Only for char/varchar return types
    for (int idx = 0; idx < arguments.length; ++idx) {
      if (arguments[idx].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentException("CONCAT only takes primitive arguments");
      }
      poi = (PrimitiveObjectInspector)arguments[idx];
      currentCategory = poi.getPrimitiveCategory();
      if (idx == 0) {
        returnType = currentCategory;
      }
      switch (currentCategory) {
        case BINARY:
          fixedLengthReturnValue = false;
          if (returnType != currentCategory) {
            // mix of binary/non-binary args
            returnType = PrimitiveCategory.STRING;
          }
          break;
        case CHAR:
        case VARCHAR:
          if (!fixedLengthReturnValue) {
            returnType = PrimitiveCategory.STRING;
          }
          if (fixedLengthReturnValue && currentCategory == PrimitiveCategory.VARCHAR) {
            returnType = PrimitiveCategory.VARCHAR;
          }
          break;
        default:
          returnType = PrimitiveCategory.STRING;
          fixedLengthReturnValue = false;
          break;
      }

      // If all arguments are of known length then we can keep track of the max
      // length of the return type. However if the return length exceeds the
      // max length for the char/varchar, then the return type reverts to string.
      if (fixedLengthReturnValue) {
        returnLength += GenericUDFUtils.StringHelper.getFixedStringSizeForType(poi);
        if ((returnType == PrimitiveCategory.VARCHAR
                && returnLength > HiveVarchar.MAX_VARCHAR_LENGTH)
            || (returnType == PrimitiveCategory.CHAR
                && returnLength > HiveChar.MAX_CHAR_LENGTH)) {
          returnType = PrimitiveCategory.STRING;
          fixedLengthReturnValue = false;
        }
      }
    }

    if (returnType == PrimitiveCategory.BINARY) {
      bw = new BytesWritable[arguments.length];
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    } else {
      // treat all inputs as string, the return value will be converted to the appropriate type.
      createStringConverters();
      returnHelper = new GenericUDFUtils.StringHelper(returnType);
      BaseCharTypeInfo typeInfo;
      switch (returnType) {
        case STRING:
          return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        case CHAR:
          typeInfo = TypeInfoFactory.getCharTypeInfo(returnLength);
          return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
        case VARCHAR:
          typeInfo = TypeInfoFactory.getVarcharTypeInfo(returnLength);
          return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
        default:
          throw new UDFArgumentException("Unexpected CONCAT return type of " + returnType);
      }
    }
  }

  private void createStringConverters() {
    stringConverters = new StringConverter[argumentOIs.length];
    for (int idx = 0; idx < argumentOIs.length; ++idx) {
      stringConverters[idx] = new StringConverter((PrimitiveObjectInspector) argumentOIs[idx]);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (returnType == PrimitiveCategory.BINARY) {
      return binaryEvaluate(arguments);
    } else {
      return returnHelper.setReturnValue(stringEvaluate(arguments));
    }
  }

  public Object binaryEvaluate(DeferredObject[] arguments) throws HiveException {
    int len = 0;
    for (int idx = 0; idx < arguments.length; ++idx) {
      bw[idx] = ((BinaryObjectInspector)argumentOIs[idx])
          .getPrimitiveWritableObject(arguments[idx].get());
      if (bw[idx] == null){
        return null;
      }
      len += bw[idx].getLength();
    }

    byte[] out = new byte[len];
    int curLen = 0;
    // Need to iterate twice since BytesWritable doesn't support append.
    for (BytesWritable bytes : bw){
      System.arraycopy(bytes.getBytes(), 0, out, curLen, bytes.getLength());
      curLen += bytes.getLength();
    }
    return new BytesWritable(out);
  }

  public String stringEvaluate(DeferredObject[] arguments) throws HiveException {
    StringBuilder sb = new StringBuilder();
    for (int idx = 0; idx < arguments.length; ++idx) {
      String val = null;
      if (arguments[idx] != null) {
        val = (String) stringConverters[idx].convert(arguments[idx].get());
      }
      if (val == null) {
        return null;
      }
      sb.append(val);
    }
    return sb.toString();
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("concat", children);
  }

}
