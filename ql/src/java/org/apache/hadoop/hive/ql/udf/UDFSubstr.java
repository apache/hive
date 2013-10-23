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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStart;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStartLen;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFSubstr.
 *
 */
@Description(name = "substr,substring",
    value = "_FUNC_(str, pos[, len]) - returns the substring of str that"
    + " starts at pos and is of length len or" +
    "_FUNC_(bin, pos[, len]) - returns the slice of byte array that"
    + " starts at pos and is of length len",
    extended = "pos is a 1-based index. If pos<0 the starting position is"
    + " determined by counting backwards from the end of str.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('Facebook', 5) FROM src LIMIT 1;\n"
    + "  'book'\n"
    + "  > SELECT _FUNC_('Facebook', -5) FROM src LIMIT 1;\n"
    + "  'ebook'\n"
    + "  > SELECT _FUNC_('Facebook', 5, 1) FROM src LIMIT 1;\n"
    + "  'b'")
@VectorizedExpressions({StringSubstrColStart.class, StringSubstrColStartLen.class})
public class UDFSubstr extends UDF {

  private final int[] index;
  private final Text r;

  public UDFSubstr() {
    index = new int[2];
    r = new Text();
  }

  public Text evaluate(Text t, IntWritable pos, IntWritable len) {

    if ((t == null) || (pos == null) || (len == null)) {
      return null;
    }

    r.clear();
    if ((len.get() <= 0)) {
      return r;
    }

    String s = t.toString();
    int[] index = makeIndex(pos.get(), len.get(), s.length());
    if (index == null) {
      return r;
    }

    r.set(s.substring(index[0], index[1]));
    return r;
  }

  private int[] makeIndex(int pos, int len, int inputLen) {
    if ((Math.abs(pos) > inputLen)) {
      return null;
    }

    int start, end;

    if (pos > 0) {
      start = pos - 1;
    } else if (pos < 0) {
      start = inputLen + pos;
    } else {
      start = 0;
    }

    if ((inputLen - start) < len) {
      end = inputLen;
    } else {
      end = start + len;
    }
    index[0] = start;
    index[1] = end;
    return index;
  }

  private final IntWritable maxValue = new IntWritable(Integer.MAX_VALUE);

  public Text evaluate(Text s, IntWritable pos) {
    return evaluate(s, pos, maxValue);
  }

  public BytesWritable evaluate(BytesWritable bw, IntWritable pos, IntWritable len) {

    if ((bw == null) || (pos == null) || (len == null)) {
      return null;
    }

    if ((len.get() <= 0)) {
      return new BytesWritable();
    }

    int[] index = makeIndex(pos.get(), len.get(), bw.getLength());
    if (index == null) {
      return new BytesWritable();
    }

    return new BytesWritable(Arrays.copyOfRange(bw.getBytes(), index[0], index[1]));
  }

  public BytesWritable evaluate(BytesWritable bw, IntWritable pos){
    return evaluate(bw, pos, maxValue);
  }
}
