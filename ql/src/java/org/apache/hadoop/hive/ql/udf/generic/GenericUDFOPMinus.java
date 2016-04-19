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
import org.apache.hadoop.hive.ql.exec.vector.expressions.DateColSubtractDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DateColSubtractDateScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DateScalarSubtractDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;


@Description(name = "-", value = "a _FUNC_ b - Returns the difference a-b")
@VectorizedExpressions({LongColSubtractLongColumn.class, LongColSubtractDoubleColumn.class,
  DoubleColSubtractLongColumn.class, DoubleColSubtractDoubleColumn.class,
  LongColSubtractLongScalar.class, LongColSubtractDoubleScalar.class,
  DoubleColSubtractLongScalar.class, DoubleColSubtractDoubleScalar.class,
  LongScalarSubtractLongColumn.class, LongScalarSubtractDoubleColumn.class,
  DoubleScalarSubtractLongColumn.class, DoubleScalarSubtractDoubleColumn.class,
  DecimalColSubtractDecimalColumn.class, DecimalColSubtractDecimalScalar.class,
  DecimalScalarSubtractDecimalColumn.class,
  IntervalYearMonthColSubtractIntervalYearMonthColumn.class,
  IntervalYearMonthColSubtractIntervalYearMonthScalar.class,
  IntervalYearMonthScalarSubtractIntervalYearMonthColumn.class,
  IntervalDayTimeColSubtractIntervalDayTimeColumn.class,
  IntervalDayTimeColSubtractIntervalDayTimeScalar.class,
  IntervalDayTimeScalarSubtractIntervalDayTimeColumn.class,
  TimestampColSubtractIntervalDayTimeColumn.class,
  TimestampColSubtractIntervalDayTimeScalar.class,
  TimestampScalarSubtractIntervalDayTimeColumn.class,
  TimestampColSubtractTimestampColumn.class,
  TimestampColSubtractTimestampScalar.class,
  TimestampScalarSubtractTimestampColumn.class,
  DateColSubtractDateColumn.class,
  DateColSubtractDateScalar.class,
  DateScalarSubtractDateColumn.class,
  DateColSubtractTimestampColumn.class,
  DateColSubtractTimestampScalar.class,
  DateScalarSubtractTimestampColumn.class,
  TimestampColSubtractDateColumn.class,
  TimestampColSubtractDateScalar.class,
  TimestampScalarSubtractDateColumn.class,
  DateColSubtractIntervalDayTimeColumn.class,
  DateColSubtractIntervalDayTimeScalar.class,
  DateScalarSubtractIntervalDayTimeColumn.class,
  DateColSubtractIntervalYearMonthColumn.class,
  DateScalarSubtractIntervalYearMonthColumn.class,
  DateColSubtractIntervalYearMonthScalar.class,
  TimestampColSubtractIntervalYearMonthColumn.class,
  TimestampScalarSubtractIntervalYearMonthColumn.class,
  TimestampColSubtractIntervalYearMonthScalar.class,
})
public class GenericUDFOPMinus extends GenericUDFBaseArithmetic {

  public GenericUDFOPMinus() {
    super();
    this.opDisplayName = "-";
  }

  @Override
  protected GenericUDFBaseNumeric instantiateNumericUDF() {
    return new GenericUDFOPNumericMinus();
  }

  @Override
  protected GenericUDF instantiateDTIUDF() {
    return new GenericUDFOPDTIMinus();
  }
}
