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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.tez.runtime.api.ProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

import java.io.IOException;

public class TestGenericUDFSurrogateKey {

  private GenericUDFSurrogateKey udf;
  private TezContext mockTezContext;
  private ProcessorContext mockProcessorContest;
  private ObjectInspector[] emptyArguments = {};

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  
  @Before
  public void init() {
    udf = new GenericUDFSurrogateKey();
    mockTezContext = Mockito.mock(TezContext.class);
    mockProcessorContest = Mockito.mock(ProcessorContext.class);
    when(mockTezContext.getTezProcessorContext()).thenReturn(mockProcessorContest);
  }

  @Test
  public void testSurrogateKeyDefault() throws HiveException {
    when(mockProcessorContest.getTaskIndex()).thenReturn(1);

    udf.initialize(emptyArguments);
    udf.configure(mockTezContext);
    udf.setWriteId(1);

    runAndVerifyConst((1L << 40) + (1L << 24), udf);
    runAndVerifyConst((1L << 40) + (1L << 24) + 1, udf);
    runAndVerifyConst((1L << 40) + (1L << 24) + 2, udf);
  }

  @Test
  public void testSurrogateKeyBitsSet() throws HiveException {
    when(mockProcessorContest.getTaskIndex()).thenReturn(1);

    udf.initialize(getArguments(10, 10));
    udf.configure(mockTezContext);
    udf.setWriteId(1);

    runAndVerifyConst((1L << 54) + (1L << 44), udf);
    runAndVerifyConst((1L << 54) + (1L << 44) + 1, udf);
    runAndVerifyConst((1L << 54) + (1L << 44) + 2, udf);
  }

  @Test
  public void testIllegalNumberOfArgs() throws HiveException {
    expectedException.expect(UDFArgumentLengthException.class);
    expectedException.expectMessage(
        "The function SURROGATE_KEY takes 0 or 2 integer arguments (write id bits, taks id bits), but found 1");

    ConstantObjectInspector argument0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.intTypeInfo, new IntWritable(10));
    ObjectInspector[] arguments = {argument0};

    udf.initialize(arguments);
  }

  @Test
  public void testWriteIdBitsOutOfRange() throws HiveException {
    expectedException.expect(UDFArgumentException.class);
    expectedException.expectMessage("Write ID bits must be between 1 and 62 (value: 63)");

    udf.initialize(getArguments(63, 10));
  }

  @Test
  public void testTaskIdBitsOutOfRange() throws HiveException {
    expectedException.expect(UDFArgumentException.class);
    expectedException.expectMessage("Task ID bits must be between 1 and 62 (value: 0)");

    udf.initialize(getArguments(10, 0));
  }

  @Test
  public void testBitSumOutOfRange() throws HiveException {
    expectedException.expect(UDFArgumentException.class);
    expectedException.expectMessage("Write ID bits + Task ID bits must be less than 63 (value: 80)");

    udf.initialize(getArguments(40, 40));
  }

  @Test
  public void testNotTezContext() throws HiveException {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("surrogate_key function is only supported if the execution engine is Tez");

    MapredContext mockContext = Mockito.mock(MapredContext.class);

    udf.initialize(emptyArguments);
    udf.configure(mockContext);
  }

  @Test
  public void testNoWriteId() throws HiveException {
    expectedException.expect(HiveException.class);
    expectedException.expectMessage("Could not obtain Write ID for the surrogate_key function");

    when(mockProcessorContest.getTaskIndex()).thenReturn(1);

    udf.initialize(emptyArguments);
    udf.configure(mockTezContext);

    runAndVerifyConst(0, udf);
  }

  @Test
  public void testWriteIdOverLimit() throws HiveException {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Write ID is out of range (10 bits) in surrogate_key");

    udf.initialize(getArguments(10, 10));
    udf.setWriteId(1 << 10);
  }

  @Test
  public void testTaskIdOverLimit() throws HiveException {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Task ID is out of range (10 bits) in surrogate_key");

    when(mockProcessorContest.getTaskIndex()).thenReturn(1 << 10);

    udf.initialize(getArguments(10, 10));
    udf.configure(mockTezContext);
  }

  @Test
  public void testRowIdOverLimit() throws HiveException {
    expectedException.expect(HiveException.class);
    expectedException.expectMessage("Row ID is out of range (1 bits) in surrogate_key");

    when(mockProcessorContest.getTaskIndex()).thenReturn(1);

    udf.initialize(getArguments(32, 31));
    udf.configure(mockTezContext);
    udf.setWriteId(1);

    runAndVerifyConst((1L << 32) + (1L << 1), udf);
    runAndVerifyConst((1L << 32) + (1L << 1) + 1, udf);
    runAndVerifyConst((1L << 32) + (1L << 1) + 2, udf);
  }

  private ObjectInspector[] getArguments(int writeIdBits, int taskIdBits) {
    ConstantObjectInspector argument0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.intTypeInfo, new IntWritable(writeIdBits));
    ConstantObjectInspector argument1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.intTypeInfo, new IntWritable(taskIdBits));
    ObjectInspector[] arguments = {argument0, argument1};
    return arguments;
  }

  private void runAndVerifyConst(long expResult, GenericUDFSurrogateKey udf)
      throws HiveException {
    DeferredObject[] args = {};
    LongWritable output = (LongWritable)udf.evaluate(args);
    assertEquals("surrogate_key() test ", expResult, output.get());
  }

  @After
  public void close() throws IOException {
    udf.close();
  }
}
