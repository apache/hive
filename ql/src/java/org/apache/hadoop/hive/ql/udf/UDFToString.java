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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class UDFToString extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToString.class.getName());

  Text t = new Text();
  ByteStream.Output out = new ByteStream.Output();

  public UDFToString() {
  }

  public Text evaluate(NullWritable i) {
    return null;
  }

  byte[] trueBytes = { 'T', 'R', 'U', 'E' };
  byte[] falseBytes = { 'F', 'A', 'L', 'S', 'E' };

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
      t.set(out.getData(), 0, out.getCount());
      return t;
    }
  }

  public Text evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyInteger.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getCount());
      return t;
    }
  }

  public Text evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyInteger.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getCount());
      return t;
    }
  }

  public Text evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      out.reset();
      LazyLong.writeUTF8NoException(out, i.get());
      t.set(out.getData(), 0, out.getCount());
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

}
