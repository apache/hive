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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncCeilDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncCeilDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncCeilLongToLong;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.LongWritable;

@Description(name = "ceil,ceiling",
value = "_FUNC_(x) - Find the smallest integer not smaller than x",
extended = "Example:\n"
    + "  > SELECT _FUNC_(-0.1) FROM src LIMIT 1;\n"
    + "  0\n"
    + "  > SELECT _FUNC_(5) FROM src LIMIT 1;\n" + "  5")
@VectorizedExpressions({FuncCeilLongToLong.class, FuncCeilDoubleToLong.class, FuncCeilDecimalToDecimal.class})
public final class GenericUDFCeil extends GenericUDFFloorCeilBase {

  public GenericUDFCeil() {
    super();
    opDisplayName = "ceil";
  }

  @Override
  protected LongWritable evaluate(DoubleWritable input) {
    longWritable.set((long) Math.ceil(input.get()));
    return longWritable;
  }

  @Override
  protected HiveDecimalWritable evaluate(HiveDecimalWritable input) {
    HiveDecimal bd = input.getHiveDecimal();
    decimalWritable.set(bd.setScale(0, HiveDecimal.ROUND_CEILING));
    return decimalWritable;
  }

}
