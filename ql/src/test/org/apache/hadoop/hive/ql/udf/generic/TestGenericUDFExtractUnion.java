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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFExtractUnion.ObjectInspectorConverter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFExtractUnion.ValueConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class TestGenericUDFExtractUnion {

  @Mock
  private ObjectInspectorConverter objectInspectorConverter;
  @Mock
  private ValueConverter valueConverter;
  @Mock
  private UnionObjectInspector unionOI;
  @Mock
  private WritableConstantIntObjectInspector tagOI;
  @Mock
  private ObjectInspector initialiseResult;
  @Mock
  private DeferredObject deferredObject;

  private final Object value = new Object();
  private final Object converted = new Object();

  private GenericUDF underTest;

  @Before
  public void before() {
    underTest = new GenericUDFExtractUnion(objectInspectorConverter, valueConverter);
  }

  @Test
  public void evaluate_SingleArgumentOnly() throws HiveException {
    when(deferredObject.get()).thenReturn(value);
    when(valueConverter.convert(value, unionOI)).thenReturn(converted);

    underTest.initialize(new ObjectInspector[] { unionOI });
    Object result = underTest.evaluate(new DeferredObject[] { deferredObject });

    assertThat(result, is(converted));
  }

  @Test
  public void evaluate_UnionAndTagArguments_MatchesTag() throws HiveException {
    when(unionOI.getObjectInspectors()).thenReturn(
        ImmutableList.<ObjectInspector> of(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
    when(tagOI.getWritableConstantValue()).thenReturn(new IntWritable(0));
    when(deferredObject.get()).thenReturn(value);
    when(unionOI.getTag(value)).thenReturn((byte) 0);
    when(unionOI.getField(value)).thenReturn("foo");

    underTest.initialize(new ObjectInspector[] { unionOI, tagOI });
    Object result = underTest.evaluate(new DeferredObject[] { deferredObject });

    assertThat(result, is((Object) "foo"));
  }

  @Test
  public void evaluate_UnionAndTagArguments_NotMatchesTag() throws HiveException {
    when(unionOI.getObjectInspectors()).thenReturn(
        ImmutableList.<ObjectInspector> of(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
    when(tagOI.getWritableConstantValue()).thenReturn(new IntWritable(0));
    when(deferredObject.get()).thenReturn(value);
    when(unionOI.getTag(value)).thenReturn((byte) 1);

    underTest.initialize(new ObjectInspector[] { unionOI, tagOI });
    Object result = underTest.evaluate(new DeferredObject[] { deferredObject });

    assertThat(result, is(nullValue()));
  }

  @Test
  public void initialize_SingleArgumentOnly() throws UDFArgumentException {
    when(objectInspectorConverter.convert(unionOI)).thenReturn(initialiseResult);

    ObjectInspector result = underTest.initialize(new ObjectInspector[] { unionOI });

    assertThat(result, is(initialiseResult));
  }

  @Test
  public void initialize_UnionAndTagArguments() throws UDFArgumentException {
    when(unionOI.getObjectInspectors()).thenReturn(
        ImmutableList.<ObjectInspector> of(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
    when(tagOI.getWritableConstantValue()).thenReturn(new IntWritable(0));

    ObjectInspector result = underTest.initialize(new ObjectInspector[] { unionOI, tagOI });

    assertThat(result, is((ObjectInspector) PrimitiveObjectInspectorFactory.writableStringObjectInspector));
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_NegativeTagThrowsException() throws UDFArgumentException {
    when(tagOI.getWritableConstantValue()).thenReturn(new IntWritable(-1));

    underTest.initialize(new ObjectInspector[] { unionOI, tagOI });
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_OutOfBoundsTagThrowsException() throws UDFArgumentException {
    when(unionOI.getObjectInspectors()).thenReturn(
        ImmutableList.<ObjectInspector> of(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
    when(tagOI.getWritableConstantValue()).thenReturn(new IntWritable(2));

    underTest.initialize(new ObjectInspector[] { unionOI, tagOI });
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_TagNotAnIntThrowsException() throws UDFArgumentException {
    underTest.initialize(new ObjectInspector[] { unionOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector });
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_UnionNotAUnionThrowsException() throws UDFArgumentException {
    underTest.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableIntObjectInspector, tagOI });
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_NoArgumentsThrowsException() throws UDFArgumentException {
    underTest.initialize(new ObjectInspector[] {});
  }

  @Test(expected = UDFArgumentException.class)
  public void initialize_TooManyArgumentsThrowsException() throws UDFArgumentException {
    underTest.initialize(
        new ObjectInspector[] { unionOI, tagOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector });
  }

}
