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

import java.time.ZoneId;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.LongWritable;

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
  private transient TimestampLocalTZObjectInspector inputTimestampLocalTzOI;
  private transient Converter inputTextConverter;
  private transient Converter patternConverter;
  private transient InstantFormatter formatter;
  private transient ZoneId timeZone;

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
      if (argument.getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentException(getName().toUpperCase() +
            " only takes string/date/timestamp types, got " + argument.getTypeName());
      }
    }

    HiveConf conf = SessionState.get() == null ? new HiveConf() : SessionState.getSessionConf();
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
        if (formatter == null) {
          formatter = InstantFormatter.ofConfiguration(conf);
        }
        break;
      case DATE:
        inputDateOI = (DateObjectInspector) arguments[0];
        break;
      case TIMESTAMP:
        inputTimestampOI = (TimestampObjectInspector) arguments[0];
        break;
      case TIMESTAMPLOCALTZ:
        inputTimestampLocalTzOI = (TimestampLocalTZObjectInspector) arguments[0];
        break;
      default:
        throw new UDFArgumentException("The function " + getName().toUpperCase()
            + " takes only string/date/timestamp/timestampwltz types. Got Type:" + arg1OI
            .getPrimitiveCategory().name());
    }
    if (timeZone == null) {
      timeZone = conf.getLocalTimeZone();
    }
  }

  @Override
  public void configure(MapredContext context) {
    if (context != null) {
      formatter = InstantFormatter.ofConfiguration(context.getJobConf());
      String timeZoneStr = HiveConf.getVar(context.getJobConf(), HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE);
      timeZone = TimestampTZUtil.parseTimeZone(timeZoneStr);
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
      try {
        final long epochSeconds;
        if (patternConverter == null) {
          epochSeconds = formatter.parse(textVal).getEpochSecond();
        } else {
          epochSeconds = formatter.parse(textVal, (String) patternConverter.convert(arguments[1].get())).getEpochSecond();
        }
        retValue.set(epochSeconds);
        return retValue;
      } catch (RuntimeException e) {
        return null;
      }
    } else if (inputDateOI != null) {
      TimestampTZ timestampTZ = TimestampTZUtil.convert(
          inputDateOI.getPrimitiveJavaObject(arguments[0].get()), timeZone);
      retValue.set(timestampTZ.getEpochSecond());
    } else if (inputTimestampOI != null)  {
      TimestampTZ timestampTZ = TimestampTZUtil.convert(
          inputTimestampOI.getPrimitiveJavaObject(arguments[0].get()), timeZone);
      retValue.set(timestampTZ.getEpochSecond());
    } else {
      TimestampTZ timestampTZ =
          inputTimestampLocalTzOI.getPrimitiveJavaObject(arguments[0].get());
      retValue.set(timestampTZ.getEpochSecond());
    }

    return retValue;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getName(),children);
  }

}
