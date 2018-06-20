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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFToString.
 *
 */
public class UDFToString extends UDF {
  private final Text t = new Text();
  private final ByteStream.Output out = new ByteStream.Output();

  public UDFToString() {
  }

  public Text evaluate(NullWritable i) {
    return null;
  }

  private final byte[] trueBytes = {'T', 'R', 'U', 'E'};
  private final byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};

  public Text evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      t.clear();
      t.set(i.get() ? trueBytes : falseBytes);
      return t;
    }
  }

  public Text evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyInteger.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getLength());
      return t;
    }
  }

  public Text evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyInteger.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getLength());
      return t;
    }
  }

  public Text evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyInteger.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getLength());
      return t;
    }
  }

  public Text evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyLong.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getLength());
      return t;
    }
  }

  public Text evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      t.set(i.toString());
      return t;
    }
  }

  public Text evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      t.set(i.toString());
      return t;
    }
  }

  public Text evaluate(Text i) {
      if (i == null) {
          return null;
      }
      i.set(i.toString());
      return i;
  }

  public Text evaluate(DateWritableV2 d) {
    if (d == null) {
      return null;
    } else {
      t.set(d.toString());
      return t;
    }
  }

  public Text evaluate(TimestampWritableV2 i) {
    if (i == null) {
      return null;
    } else {
      t.set(i.toString());
      return t;
    }
  }

  public Text evaluate(TimestampLocalTZWritable i) {
    if (i == null) {
      return null;
    } else {
      t.set(i.toString());
      return t;
    }
  }

  public Text evaluate(HiveDecimalWritable i) {
    if (i == null) {
      return null;
    } else {
      t.set(i.toString());
      return t;
    }
  }

  public Text evaluate (BytesWritable bw) {
    if (null == bw) {
      return null;
    }
    t.set(bw.getBytes(),0,bw.getLength());
    return t;
  }
}
