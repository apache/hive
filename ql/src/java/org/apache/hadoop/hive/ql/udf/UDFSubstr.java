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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStart;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringSubstrColStartLen;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimatorProvider;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFSubstr.
 *
 */
@Description(name = "substr,substring,mid",
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
public class UDFSubstr extends UDF implements StatEstimatorProvider {
  private final int[] index;
  private final Text r;

  public UDFSubstr() {
    index = new int[2];
    r = new Text();
  }

  public Text evaluate(Text t, LongWritable pos, LongWritable len) {
    if ((t == null) || (pos == null) || (len == null)) {
      return null;
    }

    long longPos = pos.get();
    long longLen = len.get();
    // If an unsupported value is seen, we don't want to return a string
    // that doesn't match what the user expects, so we return NULL (still
    // unexpected, of course, but probably better than a bad string).
    if (longPos > Integer.MAX_VALUE || longLen > Integer.MAX_VALUE ||
        longPos < Integer.MIN_VALUE || longLen < Integer.MIN_VALUE) {
      return null;
    }

    return evaluateInternal(t, (int) longPos, (int) longLen);
  }

  public Text evaluate(Text t, IntWritable pos, IntWritable len) {
    if ((t == null) || (pos == null) || (len == null)) {
      return null;
    }

    return evaluateInternal(t, pos.get(), len.get());
  }

  private Text evaluateInternal(Text t, int pos, int len) {
    r.clear();
    if ((len <= 0)) {
      return r;
    }

    String s = t.toString();
    int[] index = makeIndex(pos, len, s.length());
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

  // Even though we are using longs, substr can only deal with ints, so we use
  // the maximum int value as the maxValue
  private final LongWritable maxLongValue = new LongWritable(Integer.MAX_VALUE);

  public Text evaluate(Text s, IntWritable pos) {
    return evaluate(s, pos, maxValue);
  }

  public Text evaluate(Text s, LongWritable pos) {
    return evaluate(s, pos, maxLongValue);
  }

  public BytesWritable evaluate(BytesWritable bw, LongWritable pos, LongWritable len) {
    if ((bw == null) || (pos == null) || (len == null)) {
      return null;
    }

    long longPos = pos.get();
    long longLen = len.get();
    // If an unsupported value is seen, we don't want to return a string
    // that doesn't match what the user expects, so we return NULL (still
    // unexpected, of course, but probably better than a bad string).
    if (longPos > Integer.MAX_VALUE || longLen > Integer.MAX_VALUE ||
        longPos < Integer.MIN_VALUE || longLen < Integer.MIN_VALUE) {
      return null;
    }

    return evaluateInternal(bw, (int) longPos, (int) longLen);
  }

  public BytesWritable evaluate(BytesWritable bw, IntWritable pos, IntWritable len) {
    if ((bw == null) || (pos == null) || (len == null)) {
      return null;
    }

    return evaluateInternal(bw, pos.get(), len.get());
  }

  private BytesWritable evaluateInternal(BytesWritable bw, int pos, int len) {

    if (len <= 0) {
      return new BytesWritable();
    }

    int[] index = makeIndex(pos, len, bw.getLength());
    if (index == null) {
      return new BytesWritable();
    }

    return new BytesWritable(Arrays.copyOfRange(bw.getBytes(), index[0], index[1]));
  }

  public BytesWritable evaluate(BytesWritable bw, IntWritable pos){
    return evaluate(bw, pos, maxValue);
  }

  public BytesWritable evaluate(BytesWritable bw, LongWritable pos){
    return evaluate(bw, pos, maxLongValue);
  }

  @Override
  public StatEstimator getStatEstimator() {
    return new SubStrStatEstimator();
  }

  private static class SubStrStatEstimator implements StatEstimator {

    @Override
    public Optional<ColStatistics> estimate(List<ColStatistics> csList) {
      ColStatistics cs = csList.get(0).clone();
      // this might bad in a skewed case; consider:
      // 1 row with 1000 long string
      // 99 rows with 0 length
      // orig avg is 10
      // new avg is 5 (if substr(5)) ; but in reality it will stay ~10
      Optional<Double> start = getRangeWidth(csList.get(1).getRange());
      Range startRange = csList.get(1).getRange();
      if (startRange != null && startRange.minValue != null) {
        double newAvgColLen = cs.getAvgColLen() - startRange.minValue.doubleValue();
        if (newAvgColLen > 0) {
          cs.setAvgColLen(newAvgColLen);
        }
      }
      if (csList.size() > 2) {
        Range lengthRange = csList.get(2).getRange();
        if (lengthRange != null && lengthRange.maxValue != null) {
          Double w = lengthRange.maxValue.doubleValue();
          if (cs.getAvgColLen() > w) {
            cs.setAvgColLen(w);
          }
        }
      }
      return Optional.of(cs);
    }

    private Optional<Double> getRangeWidth(Range range) {
      if (range != null) {
        if (range.minValue != null && range.maxValue != null) {
          return Optional.of(range.maxValue.doubleValue() - range.minValue.doubleValue());
        }
      }
      return Optional.empty();
    }

  }
}
