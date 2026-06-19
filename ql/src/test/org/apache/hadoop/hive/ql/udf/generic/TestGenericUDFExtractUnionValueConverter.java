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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFExtractUnion.ValueConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class TestGenericUDFExtractUnionValueConverter {

  private final StandardUnionObjectInspector unionObjectInspector = ObjectInspectorFactory
      .getStandardUnionObjectInspector(
          Arrays.<ObjectInspector> asList(
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              PrimitiveObjectInspectorFactory.javaIntObjectInspector));

  private final Object union = new StandardUnion((byte) 0, "foo");

  private final ValueConverter underTest = new ValueConverter();

  @Test
  public void convertValue() {
    ObjectInspector inspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    Object value = "foo";

    Object result = underTest.convert(value, inspector);

    assertThat(result, is((Object) "foo"));
  }

  @Test
  public void convertList() {
    ObjectInspector inspector = ObjectInspectorFactory.getStandardListObjectInspector(unionObjectInspector);
    Object value = Arrays.asList(union);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) underTest.convert(value, inspector);

    assertThat(result.size(), is(1));
    assertThat(result.get(0), is((Object) Arrays.asList("foo", null)));
  }

  @Test
  public void convertMap() {
    ObjectInspector inspector = ObjectInspectorFactory
        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, unionObjectInspector);
    Object value = Collections.singletonMap("bar", union);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) underTest.convert(value, inspector);

    assertThat(result.size(), is(1));
    assertThat(result.get("bar"), is((Object) Arrays.asList("foo", null)));
  }

  @Test
  public void convertStruct() {
    ObjectInspector inspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(Arrays.asList("foo"), Arrays.<ObjectInspector> asList(unionObjectInspector));
    Object value = Arrays.asList(union);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) underTest.convert(value, inspector);

    assertThat(result.size(), is(1));
    assertThat(result.get(0), is((Object) Arrays.asList("foo", null)));
  }

  @Test
  public void convertUnion() {
    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) underTest.convert(union, unionObjectInspector);

    assertThat(result.size(), is(2));
    assertThat(result.get(0), is((Object) "foo"));
    assertThat(result.get(1), is(nullValue()));
  }

}
