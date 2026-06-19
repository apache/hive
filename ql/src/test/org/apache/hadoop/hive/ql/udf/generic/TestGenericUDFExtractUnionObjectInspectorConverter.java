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
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFExtractUnion.ObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class TestGenericUDFExtractUnionObjectInspectorConverter {

  private final ObjectInspector unionObjectInspector = ObjectInspectorFactory.getStandardUnionObjectInspector(
      Arrays.<ObjectInspector> asList(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private final ObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("tag_0", "tag_1"),
      Arrays.<ObjectInspector> asList(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private final ObjectInspectorConverter underTest = new ObjectInspectorConverter();

  @Test(expected = IllegalArgumentException.class)
  public void convertValue_NoUnionFound() {
    ObjectInspector inspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    underTest.convert(inspector);
  }

  @Test
  public void convertList() {
    ObjectInspector inspector = ObjectInspectorFactory.getStandardListObjectInspector(unionObjectInspector);

    ObjectInspector result = underTest.convert(inspector);

    assertThat(
        result.getTypeName(),
        is(ObjectInspectorFactory.getStandardListObjectInspector(structObjectInspector).getTypeName()));
  }

  @Test
  public void convertMap() {
    ObjectInspector inspector = ObjectInspectorFactory
        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, unionObjectInspector);

    ObjectInspector result = underTest.convert(inspector);

    assertThat(result.getTypeName(), is(
        ObjectInspectorFactory
            .getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                structObjectInspector)
            .getTypeName()));
  }

  @Test
  public void convertStruct() {
    List<String> names = Arrays.asList("foo");
    ObjectInspector inspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(names, Arrays.<ObjectInspector> asList(unionObjectInspector));

    ObjectInspector result = underTest.convert(inspector);

    assertThat(result.getTypeName(), is(
        ObjectInspectorFactory
            .getStandardStructObjectInspector(names, Arrays.<ObjectInspector> asList(structObjectInspector))
            .getTypeName()));
  }

  @Test
  public void convertUnion() {
    ObjectInspector result = underTest.convert(unionObjectInspector);

    assertThat(result, is(structObjectInspector));

    assertThat(result.getTypeName(), is(structObjectInspector.getTypeName()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void noChildUnions() {
    underTest.convert(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

}
