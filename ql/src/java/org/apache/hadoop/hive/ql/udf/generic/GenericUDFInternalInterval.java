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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.common.util.DateUtils;

/**
 * GenericUDF Class for support of "INTERVAL (expression) (DAY|YEAR|...)".
 */
@Description(name = "internal_interval", value = "_FUNC_(intervalType,intervalArg)",
  extended = "this method is not designed to be used by directly calling it - it provides internal support for 'INTERVAL (intervalArg) intervalType' constructs")

public class GenericUDFInternalInterval extends GenericUDF {

  private Map<Integer, IntervalProcessor> processorMap;

  private transient IntervalProcessor processor;
  private transient PrimitiveObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    // read operation mode
    if (!(arguments[0] instanceof ConstantObjectInspector)) {
      throw new UDFArgumentTypeException(0,
          getFuncName() + ": may only accept constant as first argument");
    }
    Integer operationMode = getConstantIntValue(arguments, 0);
    if (operationMode == null) {
      throw new UDFArgumentTypeException(0, "must supply operationmode");
    }

    processor = getProcessorMap().get(operationMode);
    if (processor == null) {
      throw new UDFArgumentTypeException(0,
          getFuncName() + ": unsupported operationMode: " + operationMode);
    }

    // check value argument
    if (arguments[1].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "The first argument to " + getFuncName() + " must be primitive");
    }

    inputOI = (PrimitiveObjectInspector) arguments[1];

    PrimitiveCategory inputCategory = inputOI.getPrimitiveCategory();

    if (!isValidInputCategory(inputCategory)) {
      throw new UDFArgumentTypeException(1, "The second argument to " + getFuncName()
          + " must be from the string group or numeric group (except:float/double)");
    }

    if (arguments[1] instanceof ConstantObjectInspector) {
      // return value as constant in case arg is constant
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
          processor.getTypeInfo(), processor.evaluate(getConstantStringValue(arguments, 1)));
    } else {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(processor.getTypeInfo());
    }
  }

  private boolean isValidInputCategory(PrimitiveCategory cat) throws UDFArgumentTypeException {
    PrimitiveGrouping inputOIGroup = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(cat);

    if (inputOIGroup == PrimitiveGrouping.STRING_GROUP) {
      return true;
    }
    if (inputOIGroup == PrimitiveGrouping.NUMERIC_GROUP) {
      switch (cat) {
      case DOUBLE:
      case FLOAT:
        return false;
      default:
        return true;
      }
    }
    return false;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String argString = PrimitiveObjectInspectorUtils.getString(arguments[1].get(), inputOI);
    if (argString == null) {
      return null;
    }
    try {
      return processor.evaluate(argString);
    } catch (Exception e) {
      throw new UDFArgumentTypeException(1, "Error parsing interval " + argString + " using:"
          + processor.getClass().getSimpleName());
    }
  }

  private static interface IntervalProcessor {

    Integer getKey();

    PrimitiveTypeInfo getTypeInfo();

    Object evaluate(String arg) throws UDFArgumentException;
  }

  private static abstract class AbstractDayTimeIntervalProcessor implements IntervalProcessor {
    private transient HiveIntervalDayTimeWritable intervalResult =
        new HiveIntervalDayTimeWritable();

    @Override
    public final PrimitiveTypeInfo getTypeInfo() {
      return TypeInfoFactory.intervalDayTimeTypeInfo;
    }

    @Override
    public final Object evaluate(String arg) throws UDFArgumentException {
      intervalResult.set(getIntervalDayTime(arg));
      return intervalResult;
    }

    abstract protected HiveIntervalDayTime getIntervalDayTime(String arg);
  }

  private static abstract class AbstractYearMonthIntervalProcessor implements IntervalProcessor {
    private transient HiveIntervalYearMonthWritable intervalResult =
        new HiveIntervalYearMonthWritable();

    @Override
    public final PrimitiveTypeInfo getTypeInfo() {
      return TypeInfoFactory.intervalYearMonthTypeInfo;
    }

    @Override
    public final Object evaluate(String arg) throws UDFArgumentException {
      intervalResult.set(getIntervalYearMonth(arg));
      return intervalResult;
    }

    abstract protected HiveIntervalYearMonth getIntervalYearMonth(String arg);
  }

  private static class IntervalDayLiteralProcessor extends AbstractDayTimeIntervalProcessor {

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_DAY_LITERAL;
    }

    @Override
    protected HiveIntervalDayTime getIntervalDayTime(String arg) {
      return new HiveIntervalDayTime(Integer.parseInt(arg), 0, 0, 0, 0);
    }
  }

  private static class IntervalHourLiteralProcessor extends AbstractDayTimeIntervalProcessor {
    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_HOUR_LITERAL;
    }

    @Override
    protected HiveIntervalDayTime getIntervalDayTime(String arg) {
      return new HiveIntervalDayTime(0, Integer.parseInt(arg), 0, 0, 0);
    }
  }

  private static class IntervalMinuteLiteralProcessor extends AbstractDayTimeIntervalProcessor {
    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_MINUTE_LITERAL;
    }

    @Override
    protected HiveIntervalDayTime getIntervalDayTime(String arg) {
      return new HiveIntervalDayTime(0, 0, Integer.parseInt(arg), 0, 0);
    }
  }

  private static class IntervalSecondLiteralProcessor extends AbstractDayTimeIntervalProcessor {

    private static final BigDecimal NANOS_PER_SEC_BD = new BigDecimal(DateUtils.NANOS_PER_SEC);

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_SECOND_LITERAL;
    }

    @Override
    protected HiveIntervalDayTime getIntervalDayTime(String arg) {
      BigDecimal bd = new BigDecimal(arg);
      BigDecimal bdSeconds = new BigDecimal(bd.toBigInteger());
      BigDecimal bdNanos = bd.subtract(bdSeconds);
      return new HiveIntervalDayTime(0, 0, 0, bdSeconds.intValueExact(),
          bdNanos.multiply(NANOS_PER_SEC_BD).intValue());
    }
  }

  private static class IntervalDayTimeLiteralProcessor extends AbstractDayTimeIntervalProcessor {

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL;
    }

    @Override
    protected HiveIntervalDayTime getIntervalDayTime(String arg) {
      return HiveIntervalDayTime.valueOf(arg);
    }
  }

  private static class IntervalYearMonthLiteralProcessor
      extends AbstractYearMonthIntervalProcessor {

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL;
    }

    @Override
    protected HiveIntervalYearMonth getIntervalYearMonth(String arg) {
      return HiveIntervalYearMonth.valueOf(arg);
    }
  }

  private static class IntervalYearLiteralProcessor extends AbstractYearMonthIntervalProcessor {

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_YEAR_LITERAL;
    }

    @Override
    protected HiveIntervalYearMonth getIntervalYearMonth(String arg) {
      return new HiveIntervalYearMonth(Integer.parseInt(arg), 0);
    }
  }

  private static class IntervalMonthLiteralProcessor extends AbstractYearMonthIntervalProcessor {

    @Override
    public Integer getKey() {
      return HiveParser.TOK_INTERVAL_MONTH_LITERAL;
    }

    @Override
    protected HiveIntervalYearMonth getIntervalYearMonth(String arg) {
      return new HiveIntervalYearMonth(0, Integer.parseInt(arg));
    }
  }

  private Map<Integer, IntervalProcessor> getProcessorMap() {

    if (processorMap != null) {
      return processorMap;
    }

    Map<Integer, IntervalProcessor> ret = new HashMap<>();
    IntervalProcessor ips[]=new IntervalProcessor[]{
        new IntervalDayTimeLiteralProcessor(),

        new IntervalDayLiteralProcessor(),
        new IntervalHourLiteralProcessor(),
        new IntervalMinuteLiteralProcessor(),
        new IntervalSecondLiteralProcessor(),

        new IntervalYearMonthLiteralProcessor(),

        new IntervalYearLiteralProcessor(),
        new IntervalMonthLiteralProcessor(),
    };

    for (IntervalProcessor ip : ips) {
      ret.put(ip.getKey(), ip);
    }

    return processorMap=ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return String.format("%s(%s)", processor.getClass().getSimpleName(), children[1]);
  }

}
