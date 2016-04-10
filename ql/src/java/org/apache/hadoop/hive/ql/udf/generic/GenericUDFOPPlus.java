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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;

/**
 * The reason that we list evaluate methods with all numeric types is for both
 * better performance and type checking (so we know int + int is still an int
 * instead of a double); otherwise a single method that takes (Number a, Number
 * b) and use a.doubleValue() == b.doubleValue() is enough.
 *
 * The case of int + double will be handled by implicit type casting using
 * UDFRegistry.implicitConvertable method.
 */
@Description(name = "+", value = "a _FUNC_ b - Returns a+b")
@VectorizedExpressions({LongColAddLongColumn.class, LongColAddDoubleColumn.class,
  DoubleColAddLongColumn.class, DoubleColAddDoubleColumn.class, LongColAddLongScalar.class,
  LongColAddDoubleScalar.class, DoubleColAddLongScalar.class, DoubleColAddDoubleScalar.class,
  LongScalarAddLongColumn.class, LongScalarAddDoubleColumn.class, DoubleScalarAddLongColumn.class,
  DoubleScalarAddDoubleColumn.class, DecimalScalarAddDecimalColumn.class, DecimalColAddDecimalColumn.class,
  DecimalColAddDecimalScalar.class,
  IntervalYearMonthColAddIntervalYearMonthColumn.class,
  IntervalYearMonthColAddIntervalYearMonthScalar.class,
  IntervalYearMonthScalarAddIntervalYearMonthColumn.class,
  IntervalDayTimeColAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddIntervalDayTimeScalar.class,
  IntervalDayTimeScalarAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddTimestampColumn.class,
  IntervalDayTimeColAddTimestampScalar.class,
  IntervalDayTimeScalarAddTimestampColumn.class,
  TimestampColAddIntervalDayTimeColumn.class,
  TimestampColAddIntervalDayTimeScalar.class,
  TimestampScalarAddIntervalDayTimeColumn.class,
  DateColAddIntervalDayTimeColumn.class,
  DateColAddIntervalDayTimeScalar.class,
  DateScalarAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddDateColumn.class,
  IntervalDayTimeColAddDateScalar.class,
  IntervalDayTimeScalarAddDateColumn.class,
  IntervalYearMonthColAddDateColumn.class,
  IntervalYearMonthColAddDateScalar.class,
  IntervalYearMonthScalarAddDateColumn.class,
  IntervalYearMonthColAddTimestampColumn.class,
  IntervalYearMonthColAddTimestampScalar.class,
  IntervalYearMonthScalarAddTimestampColumn.class,
  DateColAddIntervalYearMonthColumn.class,
  DateScalarAddIntervalYearMonthColumn.class,
  DateColAddIntervalYearMonthScalar.class,
  TimestampColAddIntervalYearMonthColumn.class,
  TimestampScalarAddIntervalYearMonthColumn.class,
  TimestampColAddIntervalYearMonthScalar.class
})
public class GenericUDFOPPlus extends GenericUDFBaseArithmetic {

  public GenericUDFOPPlus() {
    super();
    this.opDisplayName = "+";
  }

  @Override
  protected GenericUDFBaseNumeric instantiateNumericUDF() {
    return new GenericUDFOPNumericPlus();
  }

  @Override
  protected GenericUDF instantiateDTIUDF() {
    return new GenericUDFOPDTIPlus();
  }
}
