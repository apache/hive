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

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFFromUnixTime.
 *
 */
@Description(name = "from_unixtime",
    value = "_FUNC_(unix_time, format) - returns unix_time in the specified format",
    extended = "Example:\n"
        + "  > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;\n"
        + "  '1970-01-01 00:00:00'")
public class GenericUDFFromUnixTime extends GenericUDF {

  private transient IntObjectInspector inputIntOI;
  private transient LongObjectInspector inputLongOI;
  private transient Converter inputTextConverter;
  private transient ZoneId timeZone;
  private transient final Text result = new Text();

  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private transient String lastFormat = null;


  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException("The function " + getName().toUpperCase() +
          "requires at least one argument");
    }
    if (arguments.length > 2) {
      throw new UDFArgumentLengthException("Too many arguments for the function " + getName().toUpperCase());
    }
    for (ObjectInspector argument : arguments) {
      if (argument.getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentException(getName().toUpperCase() +
            " only takes primitive types, got " + argument.getTypeName());
      }
    }

    PrimitiveObjectInspector arg0OI = (PrimitiveObjectInspector) arguments[0];
    switch (arg0OI.getPrimitiveCategory()) {
      case INT:
        inputIntOI = (IntObjectInspector) arguments[0];
        break;
      case LONG:
        inputLongOI = (LongObjectInspector) arguments[0];
        break;
      default:
        throw new UDFArgumentException("The function " + getName().toUpperCase()
            + " takes only int/long types for first argument. Got Type:" + arg0OI.getPrimitiveCategory().name());
    }

    if (arguments.length == 2) {
      PrimitiveObjectInspector arg1OI = (PrimitiveObjectInspector) arguments[1];
      switch (arg1OI.getPrimitiveCategory()) {
        case CHAR:
        case VARCHAR:
        case STRING:
          inputTextConverter = ObjectInspectorConverters.getConverter(arg1OI,
              PrimitiveObjectInspectorFactory.javaStringObjectInspector);
          break;
        default:
          throw new UDFArgumentException("The function " + getName().toUpperCase()
              + " takes only string type for second argument. Got Type:" + arg1OI.getPrimitiveCategory().name());
      }
    }

    if (timeZone == null) {
      timeZone = SessionState.get().getConf().getLocalTimeZone();
      formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public void configure(MapredContext context) {
    if (context != null) {
      String timeZoneStr = HiveConf.getVar(context.getJobConf(), HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE);
      timeZone = TimestampTZUtil.parseTimeZone(timeZoneStr);
      formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    if (inputTextConverter != null) {
      if (arguments[1].get() == null) {
        return null;
      }
      String format = (String) inputTextConverter.convert(arguments[1].get());
      if (format == null) {
        return null;
      }
      if (!format.equals(lastFormat)) {
        formatter = new SimpleDateFormat(format);
        formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
        lastFormat = format;
      }
    }

    // convert seconds to milliseconds
    long unixtime;
    if (inputIntOI != null) {
      unixtime = inputIntOI.get(arguments[0].get());
    } else {
      unixtime = inputLongOI.get(arguments[0].get());
    }

    Date date = new Date(unixtime * 1000L);
    result.set(formatter.format(date));
    return result;
  }

  protected String getName() {
    return "from_unixtime";
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder(32);
    sb.append(getName());
    sb.append('(');
    sb.append(StringUtils.join(children, ", "));
    sb.append(')');
    return sb.toString();
  }

}
