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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFOPBitRightShift.
 *
 */
@Description(name = "shiftright", value = "_FUNC_(a, b) - Bitwise right shift",
    extended = "Returns int for tinyint, smallint and int a. Returns bigint for bigint a."
    + "\nExample:\n  > SELECT _FUNC_(4, 1);\n  2")
public class UDFOPBitShiftRight extends UDFBaseBitOP {

  public UDFOPBitShiftRight() {
  }

  public IntWritable evaluate(ByteWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }
    intWritable.set(a.get() >> b.get());
    return intWritable;
  }

  public IntWritable evaluate(ShortWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }
    intWritable.set(a.get() >> b.get());
    return intWritable;
  }

  public IntWritable evaluate(IntWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }
    intWritable.set(a.get() >> b.get());
    return intWritable;
  }

  public LongWritable evaluate(LongWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }
    longWritable.set(a.get() >> b.get());
    return longWritable;
  }
}
