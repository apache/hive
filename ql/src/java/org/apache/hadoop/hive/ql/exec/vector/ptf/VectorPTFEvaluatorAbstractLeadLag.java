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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorBase;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFGroupBatches;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.Range;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

public abstract class VectorPTFEvaluatorAbstractLeadLag extends VectorPTFEvaluatorBase {

  protected int amt;
  protected Object defaultValue;
  protected int defaultValueColumn = -1;
  protected Type type;

  public VectorPTFEvaluatorAbstractLeadLag(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum, Type type, int amt,
      VectorExpression defaultValueExpression) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    this.type = type;
    this.amt = amt;
    initDefaultValue(defaultValueExpression);
  }

  private void initDefaultValue(VectorExpression defaultValueExpression) {
    if (defaultValueExpression == null) {
      defaultValue = null;
    } else if (defaultValueExpression instanceof ConstantVectorExpression cve) {
      defaultValue = parseConstantDefaultValue(cve);
    } else if (defaultValueExpression instanceof IdentityExpression ie) {
      defaultValueColumn = ie.getOutputColumnNum();
    } else {
      throw new IllegalArgumentException(
          "Unexpected vector expression type for default value in lead/lag function: "
              + defaultValueExpression.getClass().getName());
    }
  }

  private Object parseConstantDefaultValue(ConstantVectorExpression cve) {
    if (cve.getIsNullValue() || cve.getValue() == null) {
      return null;
    }
    Object val = cve.getValue();
    switch (type) {
    case LONG:
      return parseLongDefault(val);
    case DOUBLE:
      return parseDoubleDefault(val);
    case DECIMAL:
      return parseDecimalDefault(val);
    default:
      throw new IllegalArgumentException("Unexpected column vector type " + type + " for lead/lag function");
    }
  }

  private long parseLongDefault(Object val) {
    if (val instanceof Number number) {
      return number.longValue();
    } else if (val instanceof HiveDecimal hivedecimal) {
      return hivedecimal.longValue();
    } else if (val instanceof HiveDecimalWritable hivedecimalwritable) {
      return hivedecimalwritable.longValue();
    } else if (val instanceof byte[] byteArray) {
      return Long.parseLong(new String(byteArray, StandardCharsets.UTF_8));
    } else if (val instanceof String string) {
      return Long.parseLong(string);
    }
    throw new IllegalArgumentException("Unexpected default value " + val + " ("
        + val.getClass().getName() + ") for column vector type " + type
        + " in lead/lag function");
  }

  private double parseDoubleDefault(Object val) {
    if (val instanceof Number number) {
      return number.doubleValue();
    } else if (val instanceof HiveDecimal hivedecimal) {
      return hivedecimal.doubleValue();
    } else if (val instanceof HiveDecimalWritable hivedecimalwritable) {
      return hivedecimalwritable.doubleValue();
    } else if (val instanceof byte[] byteArray) {
      return Double.parseDouble(new String(byteArray, StandardCharsets.UTF_8));
    } else if (val instanceof String string) {
      return Double.parseDouble(string);
    }
    throw new IllegalArgumentException("Unexpected default value " + val + " ("
        + val.getClass().getName() + ") for column vector type " + type
        + " in lead/lag function");
  }

  private HiveDecimalWritable parseDecimalDefault(Object val) {
    if (val instanceof HiveDecimal hivedecimal) {
      return new HiveDecimalWritable(hivedecimal);
    } else if (val instanceof HiveDecimalWritable hivedecimalwritable) {
      return new HiveDecimalWritable(hivedecimalwritable);
    } else if (val instanceof Double d) {
      HiveDecimalWritable dec = new HiveDecimalWritable();
      dec.setFromDouble(d);
      return dec;
    } else if (val instanceof Float f) {
      HiveDecimalWritable dec = new HiveDecimalWritable();
      dec.setFromDouble(f.doubleValue());
      return dec;
    } else if (val instanceof Number number) {
      return new HiveDecimalWritable(number.longValue());
    } else if (val instanceof byte[] byteArray) {
      return new HiveDecimalWritable(new String(byteArray, StandardCharsets.UTF_8));
    } else if (val instanceof String string) {
      return new HiveDecimalWritable(string);
    }
    throw new IllegalArgumentException("Unexpected default value " + val + " ("
        + val.getClass().getName() + ") for column vector type " + type
        + " in lead/lag function");
  }

  @Override
  public boolean canRunOptimizedCalculation(int rowNum, Range range) {
    return true;
  }

  protected Object getDefaultValue(int rowNum, VectorPTFGroupBatches batches) throws HiveException {
    if (defaultValueColumn > -1) {
      return batches.getValue(rowNum, defaultValueColumn);
    } else {
      return defaultValue;
    }
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
  }

  // TODO: HIVE-25123: Implement vectorized streaming lead/lag
  @Override
  public boolean streamsResult() {
    return false;
  }

  @Override
  public Type getResultColumnVectorType() {
    return type;
  }

  @Override
  public void resetEvaluator() {
  }

  @Override
  public boolean isCacheableForRange() {
    return false;
  }

  @Override
  public void mapCustomColumns(int[] bufferedColumnMap) {
    if (defaultValueColumn > -1) {
      defaultValueColumn = IntStream.range(0, bufferedColumnMap.length)
          .filter(j -> bufferedColumnMap[j] == defaultValueColumn).findFirst()
          .orElseGet(() -> defaultValueColumn);
    }
  }
}
