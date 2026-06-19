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
 * UDFOPBitNot.
 *
 */
@Description(name = "~", value = "_FUNC_ n - Bitwise not", extended = "Example:\n"
    + "  > SELECT _FUNC_ 0 FROM src LIMIT 1;\n" + "  -1")
public class UDFOPBitNot extends UDFBaseBitOP {

  public UDFOPBitNot() {
  }

  public ByteWritable evaluate(ByteWritable a) {
    if (a == null) {
      return null;
    }
    byteWritable.set((byte) (~a.get()));
    return byteWritable;
  }

  public ShortWritable evaluate(ShortWritable a) {
    if (a == null) {
      return null;
    }
    shortWritable.set((short) (~a.get()));
    return shortWritable;
  }

  public IntWritable evaluate(IntWritable a) {
    if (a == null) {
      return null;
    }
    intWritable.set(~a.get());
    return intWritable;
  }

  public LongWritable evaluate(LongWritable a) {
    if (a == null) {
      return null;
    }
    longWritable.set(~a.get());
    return longWritable;
  }

}
