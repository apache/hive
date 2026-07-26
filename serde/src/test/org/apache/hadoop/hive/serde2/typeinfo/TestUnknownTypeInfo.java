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

package org.apache.hadoop.hive.serde2.typeinfo;

import java.util.List;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.UnknownObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestUnknownTypeInfo {

  @Test
  public void testParseUnknownType() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.UNKNOWN_TYPE_NAME);
    Assert.assertEquals(UnknownTypeInfo.get(), typeInfo);
    Assert.assertEquals(ObjectInspector.Category.UNKNOWN, typeInfo.getCategory());
  }

  @Test
  public void testUnknownObjectInspector() {
    ObjectInspector oi = ObjectInspectorFactory.getUnknownObjectInspector();
    Assert.assertEquals(UnknownObjectInspector.get(), oi);
    Assert.assertEquals(ObjectInspector.Category.UNKNOWN, oi.getCategory());
    Assert.assertEquals(serdeConstants.UNKNOWN_TYPE_NAME, oi.getTypeName());
  }

  @Test
  public void testVoidIsCompatibleWithUnknownType() {
    Assert.assertTrue(TypeInfoUtils.isVoidCompatibleTarget(
        TypeInfoFactory.voidTypeInfo, TypeInfoFactory.getUnknownTypeInfo()));
    Assert.assertTrue(TypeInfoUtils.implicitConvertible(
        TypeInfoFactory.voidTypeInfo, TypeInfoFactory.getUnknownTypeInfo()));
  }

  @Test
  public void testLazyObjectInspectorForUnknownType() throws SerDeException {
    ObjectInspector oi = LazyFactory.createLazyStructInspector(
        List.of("placeholder"),
        List.of(TypeInfoFactory.getUnknownTypeInfo()),
        new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8},
        new Text("\\N"), false, false, (byte) 0);
    Assert.assertNotNull(oi);
  }

  @Test
  public void testStandardObjectInspectorFromTypeInfo() {
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(UnknownTypeInfo.get());
    Assert.assertEquals(UnknownObjectInspector.get(), oi);
  }

  @Test
  public void testThriftTypeMapping() {
    Assert.assertEquals(Type.UNKNOWN_TYPE, Type.getType(serdeConstants.UNKNOWN_TYPE_NAME));
    Assert.assertEquals(Type.UNKNOWN_TYPE, Type.getType(UnknownTypeInfo.get()));
  }
}
