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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFLikeAll is return true if a text(column value) matches to all patterns
 *
 * Example usage: SELECT key FROM src WHERE key like all ('%ab%', 'a%','b%','abc');
 *
 * LIKE ALL returns true if test matches all patterns patternN.
 * Returns NULL if the expression on the left hand side is NULL or if one of the patterns in the list is NULL.
 *
 */

@Description(
    name = "like all",
    value = "test _FUNC_(pattern1, pattern2...) - returns true if test matches all patterns patternN. "
        + " Returns NULL if the expression on the left hand side is NULL or if one of the patterns in the list is NULL.")
public class GenericUDFLikeAll extends GenericUDF {
  private transient PrimitiveCategory[] inputTypes;
  private transient Converter[] converters;
  private transient boolean isConstantNullPatternContain;
  private boolean isAllPatternsConstant = true;
  private final BooleanWritable bw = new BooleanWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException("The like all operator requires at least one pattern for matching, got "
          + (arguments.length - 1));
    }
    inputTypes = new PrimitiveCategory[arguments.length];
    converters = new Converter[arguments.length];

    /**expects string or null arguments */
    for (int idx = 0; idx < arguments.length; idx++) {
      checkArgPrimitive(arguments, idx);
      checkArgGroups(arguments, idx, inputTypes, PrimitiveGrouping.STRING_GROUP, PrimitiveGrouping.VOID_GROUP);
      PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[idx]).getPrimitiveCategory();
      if (arguments[idx] instanceof ConstantObjectInspector && idx != 0) {
        Object constValue = ((ConstantObjectInspector) arguments[idx]).getWritableConstantValue();
        if (!isConstantNullPatternContain && constValue == null) {
          isConstantNullPatternContain = true;
        }
      } else if (idx != 0 && isAllPatternsConstant) {
        isAllPatternsConstant = false;
      }
      converters[idx] = ObjectInspectorConverters.getConverter(arguments[idx], getOutputOI(inputType));
      inputTypes[idx] = inputType;
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    bw.set(true);

    /**If field value or any constant string pattern value is null then return null*/
    if (arguments[0].get() == null || isConstantNullPatternContain) {
      return null;
    }
    /**If all patterns are constant string and no pattern have null value the do short circuit boolean check
     * Else evaluate all patterns if any pattern contains null value then return null otherwise at last return matching result
     * */
    Text columnValue = (Text) converters[0].convert(arguments[0].get());
    Text pattern = new Text();
    UDFLike likeUdf = new UDFLike();
    for (int idx = 1; idx < arguments.length; idx++) {
      if (arguments[idx].get() == null) {
        return null;
      }
      pattern.set((Text) converters[idx].convert(arguments[idx].get()));
      if (!likeUdf.evaluate(columnValue, pattern).get() && bw.get()) {
        bw.set(false);
        if (isAllPatternsConstant) {
          return bw;
        }
      }
    }
    return bw;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("likeall", children);
  }

  private ObjectInspector getOutputOI(PrimitiveCategory inputType) {
    switch (inputType) {
      case CHAR:
      case STRING:
      case VARCHAR:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case VOID:
        return PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
      default:
        break;
    }
    return null;
  }
}
