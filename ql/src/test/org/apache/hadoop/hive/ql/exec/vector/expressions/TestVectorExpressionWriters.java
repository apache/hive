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
import java.util.ArrayList;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
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


  private Writable getWritableValue(TypeInfo ti, Timestamp value) {
    return new TimestampWritable(value);
  }

  private Writable getWritableValue(TypeInfo ti, HiveDecimal value) {
    return new HiveDecimalWritable(value);
  }

  private Writable getWritableValue(TypeInfo ti, byte[] value) {
    if (ti.equals(TypeInfoFactory.stringTypeInfo)) {
      return new Text(value);
    } else if (ti.equals(TypeInfoFactory.varcharTypeInfo)) {
      return new HiveVarcharWritable(
          new HiveVarchar(new Text(value).toString(), -1));
    } else if (ti.equals(TypeInfoFactory.binaryTypeInfo)) {
      return new BytesWritable(value);
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
  
  private void testSetterDouble(TypeInfo type) throws HiveException {
    DoubleColumnVector dcv = VectorizedRowGroupGenUtil.generateDoubleColumnVector(true, false,
        this.vectorSize, new Random(10));
    dcv.isNull[2] = true;
    Object[] values = new Object[this.vectorSize];
    
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = null;  // setValue() should be able to handle null input
      values[i] = vew.setValue(values[i], dcv, i);
      if (values[i] != null) {
        Writable expected = getWritableValue(type, dcv.vector[i]);
        Assert.assertEquals(expected, values[i]);
      } else {
        Assert.assertTrue(dcv.isNull[i]);
      }
    }
  }  

  private void testWriterDecimal(DecimalTypeInfo type) throws HiveException {
    DecimalColumnVector dcv = VectorizedRowGroupGenUtil.generateDecimalColumnVector(type, true, false,
        this.vectorSize, new Random(10));
    dcv.isNull[2] = true;
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      Writable w = (Writable) vew.writeValue(dcv, i);
      if (w != null) {
        Writable expected = getWritableValue(type, dcv.vector[i].getHiveDecimal());
        Assert.assertEquals(expected, w);
      } else {
        Assert.assertTrue(dcv.isNull[i]);
      }
    }
  }

  private void testSetterDecimal(DecimalTypeInfo type) throws HiveException {
    DecimalColumnVector dcv = VectorizedRowGroupGenUtil.generateDecimalColumnVector(type, true, false,
        this.vectorSize, new Random(10));
    dcv.isNull[2] = true;
    Object[] values = new Object[this.vectorSize];

    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = null;  // setValue() should be able to handle null input
      values[i] = vew.setValue(values[i], dcv, i);
      if (values[i] != null) {
        Writable expected = getWritableValue(type, dcv.vector[i].getHiveDecimal());
        Assert.assertEquals(expected, values[i]);
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
        Assert.assertEquals(expected, w);
      } else {
        Assert.assertTrue(lcv.isNull[i]);
      }
    }
  }
  
  private void testSetterLong(TypeInfo type) throws HiveException {
    LongColumnVector lcv = VectorizedRowGroupGenUtil.generateLongColumnVector(true, false,
        vectorSize, new Random(10));
    lcv.isNull[3] = true;

    Object[] values = new Object[this.vectorSize];
    
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = null;  // setValue() should be able to handle null input
      values[i] = vew.setValue(values[i], lcv, i);
      if (values[i] != null) {
        Writable expected = getWritableValue(type, lcv.vector[i]);
        Assert.assertEquals(expected, values[i]);
      } else {
        Assert.assertTrue(lcv.isNull[i]);
      }
    }
  }

  private void testWriterTimestamp(TypeInfo type) throws HiveException {
    Timestamp[] timestampValues = new Timestamp[vectorSize];
    TimestampColumnVector tcv =
        VectorizedRowGroupGenUtil.generateTimestampColumnVector(true, false,
        vectorSize, new Random(10), timestampValues);
    tcv.isNull[3] = true;
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      Writable w = (Writable) vew.writeValue(tcv, i);
      if (w != null) {
        Writable expected = getWritableValue(type, timestampValues[i]);
        TimestampWritable t1 = (TimestampWritable) expected;
        TimestampWritable t2 = (TimestampWritable) w;
        Assert.assertTrue(t1.equals(t2));
       } else {
        Assert.assertTrue(tcv.isNull[i]);
      }
    }
  }

  private void testSetterTimestamp(TypeInfo type) throws HiveException {
    Timestamp[] timestampValues = new Timestamp[vectorSize];
    TimestampColumnVector tcv =
        VectorizedRowGroupGenUtil.generateTimestampColumnVector(true, false,
        vectorSize, new Random(10), timestampValues);
    tcv.isNull[3] = true;

    Object[] values = new Object[this.vectorSize];

    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = null;  // setValue() should be able to handle null input
      values[i] = vew.setValue(values[i], tcv, i);
      if (values[i] != null) {
        Writable expected = getWritableValue(type, timestampValues[i]);
        TimestampWritable t1 = (TimestampWritable) expected;
        TimestampWritable t2 = (TimestampWritable) values[i];
        Assert.assertTrue(t1.equals(t2));
      } else {
        Assert.assertTrue(tcv.isNull[i]);
      }
    }
  }

  private StructObjectInspector genStructOI() {
    ArrayList<String> fieldNames1 = new ArrayList<String>();
    fieldNames1.add("theInt");
    fieldNames1.add("theBool");
    ArrayList<ObjectInspector> fieldObjectInspectors1 = new ArrayList<ObjectInspector>();
    fieldObjectInspectors1
        .add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    fieldObjectInspectors1
        .add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
    return ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames1, fieldObjectInspectors1);
  }
  
  private void testStructLong(TypeInfo type) throws HiveException {
    LongColumnVector icv = VectorizedRowGroupGenUtil.generateLongColumnVector(true, false,
        vectorSize, new Random(10));
    icv.isNull[3] = true;

    LongColumnVector bcv = VectorizedRowGroupGenUtil.generateLongColumnVector(true, false,
        vectorSize, new Random(10));
    bcv.isNull[2] = true;
    
    ArrayList<Object>[] values = (ArrayList<Object>[]) new ArrayList[this.vectorSize];
    
    StructObjectInspector soi = genStructOI();
    
    VectorExpressionWriter[] vew = VectorExpressionWriterFactory.getExpressionWriters(soi);
    
    for (int i = 0; i < vectorSize; i++) {
      values[i] = new ArrayList<Object>(2);
      values[i].add(null);
      values[i].add(null);

      vew[0].setValue(values[i], icv, i);
      vew[1].setValue(values[i], bcv, i);
      
      Object theInt = values[i].get(0);
      if (theInt == null) {
        Assert.assertTrue(icv.isNull[i]);
      } else {
        IntWritable w = (IntWritable) theInt;
        Assert.assertEquals((int) icv.vector[i], w.get());
      }

      Object theBool = values[i].get(1);
      if (theBool == null) {
        Assert.assertTrue(bcv.isNull[i]);
      } else {
        BooleanWritable w = (BooleanWritable) theBool;
        Assert.assertEquals(bcv.vector[i] == 0 ? false : true, w.get());
      }
    }
  }

  private void testWriterText(TypeInfo type) throws HiveException {
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
  
  private void testSetterText(TypeInfo type) throws HiveException {
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
    
    Object[] values = new Object[this.vectorSize];
    VectorExpressionWriter vew = getWriter(type);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = null;  // setValue() should be able to handle null input
      Writable w = (Writable) vew.setValue(values[i], bcv, i);
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
  public void testVectorExpressionSetterDouble() throws HiveException {
    testSetterDouble(TypeInfoFactory.doubleTypeInfo);
  }  

  @Test
  public void testVectorExpressionWriterFloat() throws HiveException {
    testWriterDouble(TypeInfoFactory.floatTypeInfo);
  }

  @Test
  public void testVectorExpressionSetterFloat() throws HiveException {
    testSetterDouble(TypeInfoFactory.floatTypeInfo);
  }
  
  @Test
  public void testVectorExpressionWriterLong() throws HiveException {
    testWriterLong(TypeInfoFactory.longTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterDecimal() throws HiveException {
    DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(38, 18);
    testWriterDecimal(typeInfo);
  }

  @Test
  public void testVectorExpressionSetterDecimal() throws HiveException {
    DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(38, 18);
    testSetterDecimal(typeInfo);
  }

  @Test
  public void testVectorExpressionSetterLong() throws HiveException {
    testSetterLong(TypeInfoFactory.longTypeInfo);
  }
  
  @Test
  public void testVectorExpressionStructLong() throws HiveException {
    testStructLong(TypeInfoFactory.longTypeInfo);
  }
  
  @Test
  public void testVectorExpressionWriterInt() throws HiveException {
    testWriterLong(TypeInfoFactory.intTypeInfo);
  }

  @Test
  public void testVectorExpressionSetterInt() throws HiveException {
    testSetterLong(TypeInfoFactory.intTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterShort() throws HiveException {
    testWriterLong(TypeInfoFactory.shortTypeInfo);
  }

  @Test
  public void testVectorExpressionSetterShort() throws HiveException {
    testSetterLong(TypeInfoFactory.shortTypeInfo);
  }

  
  @Test
  public void testVectorExpressionWriterBoolean() throws HiveException {
    testWriterLong(TypeInfoFactory.booleanTypeInfo);
  }
  
  @Test
  public void testVectorExpressionSetterBoolean() throws HiveException {
    testSetterLong(TypeInfoFactory.booleanTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterTimestamp() throws HiveException {
    testWriterTimestamp(TypeInfoFactory.timestampTypeInfo);
  }

  @Test
  public void testVectorExpressionSetterTimestamp() throws HiveException {
    testSetterTimestamp(TypeInfoFactory.timestampTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterByte() throws HiveException {
    testWriterLong(TypeInfoFactory.byteTypeInfo);
  }
  
  @Test
  public void testVectorExpressionSetterByte() throws HiveException {
    testSetterLong(TypeInfoFactory.byteTypeInfo);
  }

  @Test
  public void testVectorExpressionWriterString() throws HiveException {
    testWriterText(TypeInfoFactory.stringTypeInfo);
  }
  
  @Test
  public void testVectorExpressionSetterString() throws HiveException {
    testSetterText(TypeInfoFactory.stringTypeInfo);
  }
  
  @Test
  public void testVectorExpressionWriterVarchar() throws HiveException {
    testWriterText(TypeInfoFactory.varcharTypeInfo);
  }
  
  @Test
  public void testVectorExpressionSetterVarchar() throws HiveException {
    testSetterText(TypeInfoFactory.varcharTypeInfo);
  }    

  @Test
  public void testVectorExpressionWriterBinary() throws HiveException {
    testWriterText(TypeInfoFactory.binaryTypeInfo);
  }

  @Test
  public void testVectorExpressionSetterBinary() throws HiveException {
    testSetterText(TypeInfoFactory.binaryTypeInfo);
  }
}
