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

@Description(name = "-", value = "a _FUNC_ b - Returns the difference a-b")
public class GenericUDFOPDTIMinus extends GenericUDFBaseDTI {

  protected transient DateTimeMath dtm = new DateTimeMath();
  protected transient OperationType minusOpType;
  protected transient int intervalArg1Idx;
  protected transient int intervalArg2Idx;
  protected transient int dtArg1Idx;
  protected transient int dtArg2Idx;
  protected transient Converter dt1Converter;
  protected transient Converter dt2Converter;

  protected transient DateWritableV2 dateResult = new DateWritableV2();
  protected transient TimestampWritableV2 timestampResult = new TimestampWritableV2();
  protected transient HiveIntervalYearMonthWritable intervalYearMonthResult =
      new HiveIntervalYearMonthWritable();
  protected transient HiveIntervalDayTimeWritable intervalDayTimeResult =
      new HiveIntervalDayTimeWritable();

  enum OperationType {
    INTERVALYM_MINUS_INTERVALYM,
    DATE_MINUS_INTERVALYM,
    TIMESTAMP_MINUS_INTERVALYM,
    INTERVALDT_MINUS_INTERVALDT,
    TIMESTAMP_MINUS_INTERVALDT,
    TIMESTAMP_MINUS_TIMESTAMP
  };

  public GenericUDFOPDTIMinus() {
    this.opName = getClass().getSimpleName();
    this.opDisplayName = "-";
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
    // IntervalYearMonth - IntervalYearMonth = IntervalYearMonth
    // Date - IntervalYearMonth = Date (operands not reversible)
    // Timestamp - IntervalYearMonth = Timestamp (operands not reversible)
    // IntervalDayTime - IntervalDayTime = IntervalDayTime
    // Date - IntervalYearMonth = Timestamp (operands not reversible)
    // Timestamp - IntervalYearMonth = Timestamp (operands not reversible)
    // Timestamp - Timestamp = IntervalDayTime
    // Date - Date = IntervalDayTime
    // Timestamp - Date = IntervalDayTime (operands reversible)
    if (checkArgs(PrimitiveCategory.INTERVAL_YEAR_MONTH, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      minusOpType = OperationType.INTERVALYM_MINUS_INTERVALYM;
      intervalArg1Idx = 0;
      intervalArg2Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.intervalYearMonthTypeInfo);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      minusOpType = OperationType.DATE_MINUS_INTERVALYM;
      dtArg1Idx = 0;
      intervalArg1Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.dateTypeInfo);
    } else if (checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.INTERVAL_YEAR_MONTH)) {
      minusOpType = OperationType.TIMESTAMP_MINUS_INTERVALYM;
      dtArg1Idx = 0;
      intervalArg1Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
    } else if (checkArgs(PrimitiveCategory.INTERVAL_DAY_TIME, PrimitiveCategory.INTERVAL_DAY_TIME)) {
      minusOpType = OperationType.INTERVALDT_MINUS_INTERVALDT;
      intervalArg1Idx = 0;
      intervalArg2Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.intervalDayTimeTypeInfo);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.INTERVAL_DAY_TIME)
        || checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.INTERVAL_DAY_TIME)) {
      minusOpType = OperationType.TIMESTAMP_MINUS_INTERVALDT;
      dtArg1Idx = 0;
      intervalArg1Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.timestampTypeInfo);
      dt1Converter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
    } else if (checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.DATE)
        || checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.TIMESTAMP)
        || checkArgs(PrimitiveCategory.DATE, PrimitiveCategory.TIMESTAMP)
        || checkArgs(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.DATE)) {
      // Operands converted to timestamp, result as interval day-time
      minusOpType = OperationType.TIMESTAMP_MINUS_TIMESTAMP;
      dtArg1Idx = 0;
      dtArg2Idx = 1;
      resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.intervalDayTimeTypeInfo);
      dt1Converter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
      dt2Converter = ObjectInspectorConverters.getConverter(leftOI, resultOI);
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
    switch (minusOpType) {
      case INTERVALYM_MINUS_INTERVALYM: {
        HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
            arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
        HiveIntervalYearMonth iym2 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
            arguments[intervalArg2Idx].get(), inputOIs[intervalArg2Idx]);
        return handleIntervalYearMonthResult(dtm.subtract(iym1, iym2));
      }
      case DATE_MINUS_INTERVALYM: {
        HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
            arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
        Date dt1 = PrimitiveObjectInspectorUtils.getDate(
            arguments[dtArg1Idx].get(), inputOIs[dtArg1Idx]);
        return handleDateResult(dtm.subtract(dt1, iym1));
      }
      case TIMESTAMP_MINUS_INTERVALYM: {
        HiveIntervalYearMonth iym1 = PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
            arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
        Timestamp ts1 = PrimitiveObjectInspectorUtils.getTimestamp(
            arguments[dtArg1Idx].get(), inputOIs[dtArg1Idx]);
        return handleTimestampResult(dtm.subtract(ts1, iym1));
      }
      case INTERVALDT_MINUS_INTERVALDT: {
        HiveIntervalDayTime idt1 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
            arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
        HiveIntervalDayTime idt2 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
            arguments[intervalArg2Idx].get(), inputOIs[intervalArg2Idx]);
        return handleIntervalDayTimeResult(dtm.subtract(idt1, idt2));
      }
      case TIMESTAMP_MINUS_INTERVALDT: {
        HiveIntervalDayTime idt1 = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
            arguments[intervalArg1Idx].get(), inputOIs[intervalArg1Idx]);
        Timestamp ts1 = PrimitiveObjectInspectorUtils.getTimestamp(
            arguments[dtArg1Idx].get(), inputOIs[dtArg1Idx]);
        return handleTimestampResult(dtm.subtract(ts1, idt1));
      }
      case TIMESTAMP_MINUS_TIMESTAMP: {
        Timestamp ts1 = PrimitiveObjectInspectorUtils.getTimestamp(
            arguments[dtArg1Idx].get(), inputOIs[dtArg1Idx]);
        Timestamp ts2 = PrimitiveObjectInspectorUtils.getTimestamp(
            arguments[dtArg2Idx].get(), inputOIs[dtArg2Idx]);
        return handleIntervalDayTimeResult(dtm.subtract(ts1, ts2));
      }
      default:
        throw new HiveException("Unknown PlusOpType " + minusOpType);
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
