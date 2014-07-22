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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * This class can be used for math based UDFs that only have an evaluate method for {@code doubles}. By extending from
 * this class these UDFs will automatically support decimals as well.
 */
public abstract class UDFMath extends UDF {

  private final DoubleWritable doubleWritable = new DoubleWritable();

  /**
   * For subclass to implement.
   */
  protected abstract DoubleWritable doEvaluate(DoubleWritable a);

  /**
   * Returns {@code null} if the passed in value is {@code} and passes on to {@link #doEvaluate(DoubleWritable)} if not.
   */
  public final DoubleWritable evaluate(DoubleWritable a) {
    if (a == null) {
      return null;
    }

    return doEvaluate(a);
  }

  /**
   * Convert HiveDecimal to a double and call evaluate() on it.
   */
  public final DoubleWritable evaluate(HiveDecimalWritable writable) {
    if (writable == null) {
      return null;
    }

    double d = writable.getHiveDecimal().bigDecimalValue().doubleValue();
    doubleWritable.set(d);
    return doEvaluate(doubleWritable);
  }

}
