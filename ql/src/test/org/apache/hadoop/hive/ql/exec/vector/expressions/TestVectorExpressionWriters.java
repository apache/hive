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

package org.apache.hadoop.hive.ql.exec.vector.expressions;


import java.sql.Timestamp;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

/**
 * Unit tests for vector expression writers.
 */
public class TestVectorExpressionWriters {

  private final int vectorSize = 5;

  private VectorExpressionWriter getWriter(TypeInfo colTypeInfo) throws HiveException {
    ExprNodeDesc columnDesc = new ExprNodeColumnDesc();
    columnDesc.setTypeInfo(colTypeInfo);
    VectorExpressionWriter vew = VectorExpressionWriterFactory
        .genVectorExpressionWritable(columnDesc);
    return vew;
  }

  private Writable getWritableValue(TypeInfo ti, double value) {
    if (ti.equals(TypeInfoFactory.floatTypeInfo)) {
      return new FloatWritable((float) value);
    } else if (ti.equals(TypeInfoFactory.doubleTypeInfo)) {
      return new DoubleWritable(value);
    }
    return null;
  }

  private Writable getWritableValue(TypeInfo ti, byte[] value) {
    if (ti.equals(TypeInfoFactory.stringTypeInfo)) {
      return new Text(value);
    }
    return null;
  }

  private Writable getWritableValue(TypeInfo ti, long value) {
    if (ti.equals(TypeInfoFactory.byteTypeInfo)) {
      return new ByteWritable((byte) value);
    } else if (ti.equals(TypeInfoFactory.shortTypeInfo)) {
      return new ShortWritable((short) value);
    } else if (ti.equals(TypeInfoFactory.intTypeInfo)) {
      return new IntWritable( (int) value);
    } else if (ti.equals(TypeInfoFactory.longTypeInfo)) {
      return new LongWritable( (long) value);
    } else if (ti.equals(TypeInfoFactory.booleanTypeInfo)) {
      return new BooleanWritable( value == 0 ? false : true);
    } else if (ti.equals(TypeInfoFactory.timestampTypeInfo)) {
      Timestamp ts = new Timestamp(value);
      TimestampUtils.assignTimeInNanoSec(value, ts);
      TimestampWritable tw = new TimestampWritable(ts);
      return tw;
    }
    return null;
  }

  private void testWriterDouble(TypeInfo type) throws HiveException {
    DoubleColumnVector dcv = VectorizedRowGroupGenUtil.generateDoubleColumnVector(true, false,
        this.vectorSize, new Random(10));
    dcv.isNull[2] = true;
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      Writable w = (Writable) vew.writeValue(dcv, i);
      if (w != null) {
        Writable expected = getWritableValue(type, dcv.vector[i]);
        Assert.assertEquals(expected, w);
      } else {
        Assert.assertTrue(dcv.isNull[i]);
      }
    }
  }

  private void testWriterLong(TypeInfo type) throws HiveException {
    LongColumnVector lcv = VectorizedRowGroupGenUtil.generateLongColumnVector(true, false,
        vectorSize, new Random(10));
    lcv.isNull[3] = true;
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      Writable w = (Writable) vew.writeValue(lcv, i);
      if (w != null) {
        Writable expected = getWritableValue(type, lcv.vector[i]);
        if (expected instanceof TimestampWritable) {
          TimestampWritable t1 = (TimestampWritable) expected;
          TimestampWritable t2 = (TimestampWritable) w;
          Assert.assertTrue(t1.getNanos() == t2.getNanos());
          Assert.assertTrue(t1.getSeconds() == t2.getSeconds());
          continue;
        }
        Assert.assertEquals(expected, w);
      } else {
        Assert.assertTrue(lcv.isNull[i]);
      }
    }
  }

  private void testWriterBytes(TypeInfo type) throws HiveException {
    Text t1 = new Text("alpha");
    Text t2 = new Text("beta");
    BytesColumnVector bcv = new BytesColumnVector(vectorSize);
    bcv.noNulls = false;
    bcv.initBuffer();
    bcv.setVal(0, t1.getBytes(), 0, t1.getLength());
    bcv.isNull[1] = true;
    bcv.setVal(2, t2.getBytes(), 0, t2.getLength());
    bcv.isNull[3] = true;
    bcv.setVal(4, t1.getBytes(), 0, t1.getLength());
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      Writable w = (Writable) vew.writeValue(bcv, i);
      if (w != null) {
        byte [] val = new byte[bcv.length[i]];
        System.arraycopy(bcv.vector[i], bcv.start[i], val, 0, bcv.length[i]);
        Writable expected = getWritableValue(type, val);
        Assert.assertEquals(expected, w);
      } else {
        Assert.assertTrue(bcv.isNull[i]);
      }
    }
  }

  @Test
  public void testVectorExpressionWriterDouble() throws HiveException {
    testWriterDouble(TypeInfoFactory.doubleTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterFloat() throws HiveException {
    testWriterDouble(TypeInfoFactory.floatTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterLong() throws HiveException {
    testWriterLong(TypeInfoFactory.longTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterInt() throws HiveException {
    testWriterLong(TypeInfoFactory.intTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterShort() throws HiveException {
    testWriterLong(TypeInfoFactory.shortTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterBoolean() throws HiveException {
    testWriterLong(TypeInfoFactory.booleanTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterTimestamp() throws HiveException {
    testWriterLong(TypeInfoFactory.timestampTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterBye() throws HiveException {
    testWriterLong(TypeInfoFactory.byteTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterBytes() throws HiveException {
    testWriterBytes(TypeInfoFactory.stringTypeInfo);
  }
}
