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
package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * VectorPTFEvaluatorHelper is a helper class for vector ptf operations.
 */
public class VectorPTFEvaluatorHelper {

  public static Long computeValue(Long number) {
    return number == null ? 0 : number;
  }

  public static Long plus(Long l1, Long l2) {
    return l1 + l2;
  }

  public static Long minus(Long l1, Long l2) {
    return l1 - l2;
  }

  public static Double divide(Long number, long divisor) {
    if (divisor == 0) {
      return null;
    }
    return number / (double) divisor;
  }

  public static Double computeValue(Double number) {
    return number == null ? 0 : number;
  }

  public static Double plus(Double d1, Double d2) {
    return d1 + d2;
  }

  public static Double minus(Double d1, Double d2) {
    return d1 - d2;
  }

  public static Double divide(Double number, long divisor) {
    if (divisor == 0) {
      return null;
    }
    return number / (double) divisor;
  }

  public static HiveDecimalWritable computeValue(HiveDecimalWritable number) {
    return number == null ? new HiveDecimalWritable(0L)
      : new HiveDecimalWritable(number.isSet() ? number : new HiveDecimalWritable(0L));
  }

  public static HiveDecimalWritable plus(HiveDecimalWritable t1, HiveDecimalWritable t2) {
    HiveDecimalWritable result = new HiveDecimalWritable(t1);
    result.mutateAdd(t2);
    return result;
  }

  public static HiveDecimalWritable minus(HiveDecimalWritable t1, HiveDecimalWritable t2) {
    HiveDecimalWritable result = new HiveDecimalWritable(t2);
    result.mutateNegate();
    result.mutateAdd(t1);
    return result;
  }

  public static HiveDecimalWritable divide(HiveDecimalWritable number, long divisor) {
    if (number == null || divisor == 0) {
      return null;
    }

    HiveDecimalWritable result = new HiveDecimalWritable(number);
    result.mutateDivide(HiveDecimal.create(divisor));
    return result;
  }
}
