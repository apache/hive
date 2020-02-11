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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFOPBitOr.
 *
 */
@Description(name = "|", value = "a _FUNC_ b - Bitwise or", extended = "Example:\n"
    + "  > SELECT 3 _FUNC_ 5 FROM src LIMIT 1;\n" + "  7")
public class UDFOPBitOr extends UDFBaseBitOP {

  public UDFOPBitOr() {
  }

  public ByteWritable evaluate(ByteWritable a, ByteWritable b) {
    if (a == null || b == null) {
      return null;
    }
    byteWritable.set((byte) (a.get() | b.get()));
    return byteWritable;
  }

  public ShortWritable evaluate(ShortWritable a, ShortWritable b) {
    if (a == null || b == null) {
      return null;
    }
    shortWritable.set((short) (a.get() | b.get()));
    return shortWritable;
  }

  public IntWritable evaluate(IntWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }
    intWritable.set(a.get() | b.get());
    return intWritable;
  }

  public LongWritable evaluate(LongWritable a, LongWritable b) {
    if (a == null || b == null) {
      return null;
    }
    longWritable.set(a.get() | b.get());
    return longWritable;
  }

}
