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

import java.sql.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(
    name = ">",
    value = "a _FUNC_ b - Returns TRUE if a is greater than b"
)
public class UDFOPGreaterThan extends UDFBaseCompare {

  private static Log LOG = LogFactory.getLog(UDFOPGreaterThan.class.getName());

  BooleanWritable resultCache;
  public UDFOPGreaterThan() {
    resultCache = new BooleanWritable();
  }

  public BooleanWritable evaluate(Text a, Text b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(ShimLoader.getHadoopShims().compareText(a, b) > 0);
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }

  public BooleanWritable evaluate(ByteWritable a, ByteWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }

  public BooleanWritable evaluate(ShortWritable a, ShortWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }

  public BooleanWritable evaluate(IntWritable a, IntWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
  
  public BooleanWritable evaluate(LongWritable a, LongWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
  
  public BooleanWritable evaluate(FloatWritable a, FloatWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }  

  public BooleanWritable evaluate(DoubleWritable a, DoubleWritable b)  {
    BooleanWritable r = this.resultCache;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r.set(a.get() > b.get());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
}
