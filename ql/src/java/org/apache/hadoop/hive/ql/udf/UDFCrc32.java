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

import java.util.zip.CRC32;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFCrc32.
 *
 */
@Description(name = "crc32",
    value = "_FUNC_(str or bin) - Computes a cyclic redundancy check value "
    + "for string or binary argument and returns bigint value.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('ABC');\n"
    + "  2743272264\n"
    + "  > SELECT _FUNC_(binary('ABC'));\n"
    + "  2743272264")
public class UDFCrc32 extends UDF {

  private final LongWritable result = new LongWritable();
  private final CRC32 crc32 = new CRC32();

  /**
   * CRC32 for string
   */
  public LongWritable evaluate(Text n) {
    if (n == null) {
      return null;
    }

    crc32.reset();
    crc32.update(n.getBytes(), 0, n.getLength());

    result.set(crc32.getValue());
    return result;
  }

  /**
   * CRC32 for binary
   */
  public LongWritable evaluate(BytesWritable b) {
    if (b == null) {
      return null;
    }

    crc32.reset();
    crc32.update(b.getBytes(), 0, b.getLength());

    result.set(crc32.getValue());
    return result;
  }
}
