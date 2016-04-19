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

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * deterministic version of UDFUnixTimeStamp. enforces argument
 */
@Description(name = "to_unix_timestamp",
    value = "_FUNC_(date[, pattern]) - Returns the UNIX timestamp",
    extended = "Converts the specified time to number of seconds since 1970-01-01.")
@VectorizedExpressions({VectorUDFUnixTimeStampDate.class, VectorUDFUnixTimeStampString.class, VectorUDFUnixTimeStampTimestamp.class})
public class GenericUDFToUnixTimeStamp extends GenericUDF {

  private transient DateObjectInspector inputDateOI;
  private transient TimestampObjectInspector inputTimestampOI;
  private transient Converter inputTextConverter;
  private transient Converter patternConverter;

  private transient String lasPattern = "yyyy-MM-dd HH:mm:ss";
  private transient final SimpleDateFormat formatter = new SimpleDateFormat(lasPattern);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    initializeInput(arguments);
    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  protected void initializeInput(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException("The function " + getName().toUpperCase() +
          "requires at least one argument");
    }
    for (ObjectInspector argument : arguments) {
      if (arguments[0].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentException(getName().toUpperCase() +
            " only takes string/date/timestamp types, got " + argument.getTypeName());
      }
    }

    PrimitiveObjectInspector arg1OI = (PrimitiveObjectInspector) arguments[0];
    switch (arg1OI.getPrimitiveCategory()) {
      case CHAR:
      case VARCHAR:
      case STRING:
        inputTextConverter = ObjectInspectorConverters.getConverter(arg1OI,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        if (arguments.length > 1) {
          PrimitiveObjectInspector arg2OI = (PrimitiveObjectInspector) arguments[1];
          if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(arg2OI.getPrimitiveCategory())
              != PrimitiveGrouping.STRING_GROUP) {
            throw new UDFArgumentException(
              "The time pattern for " + getName().toUpperCase() + " should be string type");
          }
          patternConverter = ObjectInspectorConverters.getConverter(arg2OI,
              PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        break;

      case DATE:
        inputDateOI = (DateObjectInspector) arguments[0];
        break;
      case TIMESTAMP:
        inputTimestampOI = (TimestampObjectInspector) arguments[0];
        break;
      default:
        throw new UDFArgumentException(
            "The function " + getName().toUpperCase() + " takes only string/date/timestamp types");
    }
  }

  protected String getName() {
    return "to_unix_timestamp";
  }

  protected transient final LongWritable retValue = new LongWritable();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    if (inputTextConverter != null) {
      String textVal = (String) inputTextConverter.convert(arguments[0].get());
      if (textVal == null) {
        return null;
      }
      if (patternConverter != null) {
        if (arguments[1].get() == null) {
          return null;
        }
        String patternVal = (String) patternConverter.convert(arguments[1].get());
        if (patternVal == null) {
          return null;
        }
        if (!patternVal.equals(lasPattern)) {
          formatter.applyPattern(patternVal);
          lasPattern = patternVal;
        }
      }
      try {
        retValue.set(formatter.parse(textVal).getTime() / 1000);
        return retValue;
      } catch (ParseException e) {
        return null;
      }
    } else if (inputDateOI != null) {
      retValue.set(inputDateOI.getPrimitiveWritableObject(arguments[0].get())
                   .getTimeInSeconds());
      return retValue;
    }
    Timestamp timestamp = inputTimestampOI.getPrimitiveJavaObject(arguments[0].get());
    setValueFromTs(retValue, timestamp);
    return retValue;
  }

  protected static void setValueFromTs(LongWritable value, Timestamp timestamp) {
    value.set(timestamp.getTime() / 1000);
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder(32);
    sb.append(getName());
    sb.append('(');
    sb.append(StringUtils.join(children, ','));
    sb.append(')');
    return sb.toString();
  }
}
