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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hive.common.HiveCompat;
import org.apache.hive.common.HiveCompat.CompatLevel;

/**
 * Wrapper UDF that will instantiate the proper arithmetic UDF (numeric, date, etc)
 * depending on the argument types
 */
public abstract class GenericUDFBaseArithmetic extends GenericUDFBaseBinary {
  GenericUDF arithmeticOperation;

  // Values needed for numeric arithmetic UDFs
  protected boolean confLookupNeeded = true;
  protected boolean ansiSqlArithmetic = false;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    if (arguments.length != 2) {
      throw new UDFArgumentException(getClass().getSimpleName() + " requires two arguments.");
    }

    // Lookup values needed for numeric arithmetic UDFs
    if (confLookupNeeded) {
      CompatLevel compatLevel = HiveCompat.getCompatLevel(SessionState.getSessionConf());
      ansiSqlArithmetic = compatLevel.ordinal() > CompatLevel.HIVE_0_12.ordinal();
      confLookupNeeded = false;
    }

    // Determine if we are dealing with a numeric or date arithmetic operation
    boolean isDateTimeOp = false;
    for (int idx = 0; idx < 2; ++idx) {
      switch (((PrimitiveObjectInspector) arguments[idx]).getPrimitiveCategory()) {
        case DATE:
        case TIMESTAMP:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
          isDateTimeOp = true;
          break;
        default:
          break;
      }
    }

    if (isDateTimeOp) {
      arithmeticOperation = instantiateDTIUDF();
    } else {
      GenericUDFBaseNumeric numericUDF = instantiateNumericUDF();

      // Set values needed for numeric arithmetic UDFs
      numericUDF.setAnsiSqlArithmetic(ansiSqlArithmetic);
      numericUDF.setConfLookupNeeded(confLookupNeeded);
      arithmeticOperation = numericUDF;
    }

    return arithmeticOperation.initialize(arguments);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return arithmeticOperation.evaluate(arguments);
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    GenericUDFBaseArithmetic other = (GenericUDFBaseArithmetic) newInstance;
    other.confLookupNeeded = this.confLookupNeeded;
    other.ansiSqlArithmetic = this.ansiSqlArithmetic;
  }

  /**
   * Instantiate numeric version of the arithmetic UDF
   * @return arithmetic UDF for numeric types
   */
  protected abstract GenericUDFBaseNumeric instantiateNumericUDF();

  /**
   * Instantiate date-time/interval version of the arithmetic UDF
   * @return arithmetic UDF for date-time/interval types
   */
  protected abstract GenericUDF instantiateDTIUDF();
}
