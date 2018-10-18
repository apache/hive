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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.NoMatchingMethodException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@Description(name = "+", value = "a _FUNC_ b - Returns a+b")
public class GenericUDFOPDTIPlus extends GenericUDFBaseDTI {

  protected transient DateTimeMath dtm = new DateTimeMath();
  protected transient OperationType plusOpType;
  protected transient int intervalArg1Idx;
  protected transient int intervalArg2Idx;
  protected transient int dtArgIdx;
  protected transient Converter dtConverter;

  protected transient TimestampWritableV2 timestampResult = new TimestampWritableV2();
  protected transient DateWritableV2 dateResult = new DateWritableV2();
  protected transient HiveIntervalDayTimeWritable intervalDayTimeResult =
      new HiveIntervalDayTimeWritable();
  protected transient HiveIntervalYearMonthWritable intervalYearMonthResult =
      new HiveIntervalYearMonthWritable();

  enum OperationType {
    INTERVALYM_PLUS_INTERVALYM,
    INTERVALYM_PLUS_DATE,
    INTERVALYM_PLUS_TIMESTAMP,
    INTERVALDT_PLUS_INTERVALDT,
    INTERVALDT_PLUS_TIMESTAMP,
    DATE_PLUS_INT,
  };

  public GenericUDFOPDTIPlus() {
    this.opName = getClass().getSimpleName();
    this.opDisplayName = "+";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException(opName + " requires two arguments.");
    }

    PrimitiveObjectInspector resultOI = null;

    for (int i = 0; i < 2; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of " + opName + "  is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    inputOIs = new PrimitiveObjectInspector[] {
        (PrimitiveObjectInspector) arguments[0],
        (PrimitiveObjectInspector) arguments[1]
    };
    PrimitiveObjectInspector leftOI = inputOIs[0];
    PrimitiveObjectInspector rightOI = inputOIs[1];

    // Allowed operations:
    // IntervalYearMonth + IntervalYearMonth = IntervalYearMonth
    // IntervalYearMonth + Date = Date (operands reversible)
    // IntervalYearMonth + Timestamp = Timestamp (operands reversible)
    // IntervalDayTime + IntervalDayTime = IntervalDayTime
    // IntervalDayTime + Date = Timestamp (operands reversible)
    // IntervalDayTime + Timestamp = Timestamp (operands reversible)
    // Date + Int = Date
    if (checkArgs(PrimitiveCategory.INTERVAL_YEAR_MONTH, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      plusOpType = OperationType.INTERVALYM_PLUS_INTERVALYM;
      intervalArg1Idx = 0;
      intervalArg2Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.intervalYearMonthTypeInfo);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      plusOpType = OperationType.INTERVALYM_PLUS_DATE;
      dtArgIdx = 0;
      intervalArg1Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.dateTypeInfo);
    } else if (checkArgs(PrimitiveCategory.INTERVAL_YEAR_MONTH, PrimitiveCategory.DATE)) {
      plusOpType = OperationType.INTERVALYM_PLUS_DATE;
      intervalArg1Idx = 0;
      dtArgIdx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.dateTypeInfo);
    } else if (checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      plusOpType = OperationType.INTERVALYM_PLUS_TIMESTAMP;
      dtArgIdx = 0;
      intervalArg1Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
    } else if (checkArgs(PrimitiveCategory.INTERVAL_YEAR_MONTH, PrimitiveCategory.TIMESTAMP)) {
      plusOpType = OperationType.INTERVALYM_PLUS_TIMESTAMP;
      intervalArg1Idx = 0;
      dtArgIdx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
    } else if (checkArgs(PrimitiveCategory.INTERVAL_DAY_TIME, PrimitiveCategory.INTERVAL_DAY_TIME)) {
      plusOpType = OperationType.INTERVALDT_PLUS_INTERVALDT;
      intervalArg1Idx = 0;
      intervalArg2Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.intervalDayTimeTypeInfo);
    } else if (checkArgs(PrimitiveCategory.INTERVAL_DAY_TIME, PrimitiveCategory.DATE)
        || checkArgs(PrimitiveCategory.INTERVAL_DAY_TIME, PrimitiveCategory.TIMESTAMP)) {
      plusOpType = OperationType.INTERVALDT_PLUS_TIMESTAMP;
      intervalArg1Idx = 0;
      dtArgIdx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
      dtConverter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.INTERVAL_DAY_TIME)
        || checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.INTERVAL_DAY_TIME)) {
      plusOpType = OperationType.INTERVALDT_PLUS_TIMESTAMP;
      intervalArg1Idx = 1;
      dtArgIdx = 0;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
      dtConverter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.INT)) {
      plusOpType = OperationType.DATE_PLUS_INT;
      intervalArg1Idx = 1;
      dtArgIdx = 0;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.dateTypeInfo);
      dtConverter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
    } else {
      // Unsupported types - error
      List<TypeInfo> argTypeInfos = new ArrayList<TypeInfo>(2);
      argTypeInfos.add(leftOI.getTypeInfo());
      argTypeInfos.add(rightOI.getTypeInfo());
      throw new NoMatchingMethodException(this.getClass(), argTypeInfos, null);
    }

    return resultOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    switch (plusOpType) {
    case INTERVALYM_PLUS_INTERVALYM: {
      HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
          arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
      HiveIntervalYearMonth iym2 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
          arguments[intervalArg2Idx].get(), inputOIs[intervalArg2Idx]);
      return handleIntervalYearMonthResult(dtm.add(iym1, iym2));
    }
    case INTERVALYM_PLUS_DATE: {
      HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
          arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
      Date dt1 = PrimitiveObjectInspectorUtils.getDate(
          arguments[dtArgIdx].get(), inputOIs[dtArgIdx]);
      return handleDateResult(dtm.add(dt1, iym1));
    }
    case INTERVALYM_PLUS_TIMESTAMP: {
      HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
          arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
      Timestamp ts1 = PrimitiveObjectInspectorUtils.getTimestamp(
          arguments[dtArgIdx].get(), inputOIs[dtArgIdx]);
      return handleTimestampResult(dtm.add(ts1, iym1));
    }
    case INTERVALDT_PLUS_INTERVALDT: {
      HiveIntervalDayTime idt1 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
          arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
      HiveIntervalDayTime idt2 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
          arguments[intervalArg2Idx].get(), inputOIs[intervalArg2Idx]);
      return handleIntervalDayTimeResult(dtm.add(idt1, idt2));
    }
    case INTERVALDT_PLUS_TIMESTAMP: {
      HiveIntervalDayTime idt1 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
          arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
      Timestamp ts1 = PrimitiveObjectInspectorUtils.getTimestamp(
          arguments[dtArgIdx].get(), inputOIs[dtArgIdx]);
      return handleTimestampResult(dtm.add(ts1, idt1));
    }
    case DATE_PLUS_INT: {
      int intVal = PrimitiveObjectInspectorUtils.getInt(arguments[intervalArg1Idx].get(),
          inputOIs[intervalArg1Idx]);
      Date dt1 = PrimitiveObjectInspectorUtils.getDate(
          arguments[dtArgIdx].get(), inputOIs[dtArgIdx]);
      return handleDateResult(dtm.add(dt1, intVal));
    }
    default:
      throw new HiveException("Unknown PlusOpType " + plusOpType);
    }
  }

  protected DateWritableV2 handleDateResult(Date result) {
    if (result == null) {
      return null;
    }
    dateResult.set(result);
    return dateResult;
  }

  protected TimestampWritableV2 handleTimestampResult(Timestamp result) {
    if (result == null) {
      return null;
    }
    timestampResult.set(result);
    return timestampResult;
  }

  protected HiveIntervalYearMonthWritable handleIntervalYearMonthResult(
      HiveIntervalYearMonth result) {
    if (result == null) {
      return null;
    }
    intervalYearMonthResult.set(result);
    return intervalYearMonthResult;
  }

  protected HiveIntervalDayTimeWritable handleIntervalDayTimeResult(
      HiveIntervalDayTime result) {
    if (result == null) {
      return null;
    }
    intervalDayTimeResult.set(result);
    return intervalDayTimeResult;
  }
}
