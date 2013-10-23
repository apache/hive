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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncFloorDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncFloorLongToLong;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFFloor.
 *
 */
@Description(name = "floor",
    value = "_FUNC_(x) - Find the largest integer not greater than x",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(-0.1) FROM src LIMIT 1;\n"
    + "  -1\n"
    + "  > SELECT _FUNC_(5) FROM src LIMIT 1;\n" + "  5")
@VectorizedExpressions({FuncFloorLongToLong.class, FuncFloorDoubleToLong.class})
public class UDFFloor extends UDF {
  private final LongWritable result = new LongWritable();
  private final HiveDecimalWritable bdResult = new HiveDecimalWritable();

  public UDFFloor() {
  }

  public LongWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      result.set((long) Math.floor(i.get()));
      return result;
    }
  }

  public HiveDecimalWritable evaluate(HiveDecimalWritable i) {
    if (i == null) {
      return null;
    } else {
      HiveDecimal bd = i.getHiveDecimal();
      int origScale = bd.scale();
      bdResult.set(bd.setScale(0, HiveDecimal.ROUND_FLOOR).setScale(origScale));
      return bdResult;
    }
  }
}
