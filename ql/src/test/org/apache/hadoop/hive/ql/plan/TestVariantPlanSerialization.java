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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VariantTypeInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestVariantPlanSerialization {

  private static <T extends ExprNodeDesc> T kryoRoundTrip(T desc, Class<T> type) {
    return SerializationUtilities.deserializeObjectFromKryo(SerializationUtilities.serializeObjectToKryo(desc), type);
  }

  @Test
  void variantNullConstantSurvivesKryo() {
    ExprNodeConstantDesc copy = kryoRoundTrip(
        new ExprNodeConstantDesc(TypeInfoFactory.getVariantTypeInfo(), null), ExprNodeConstantDesc.class);
    assertEquals(VariantTypeInfo.get(), copy.getTypeInfo());
    ConstantObjectInspector oi = copy.getWritableObjectInspector();
    assertEquals(Category.VARIANT, oi.getCategory());
    assertNull(oi.getWritableConstantValue());
  }

  @Test
  void variantColumnSurvivesKryo() {
    ExprNodeColumnDesc copy = kryoRoundTrip(
        new ExprNodeColumnDesc(TypeInfoFactory.getVariantTypeInfo(), "v", "t", false), ExprNodeColumnDesc.class);
    assertEquals(Category.VARIANT, copy.getTypeInfo().getCategory());
    assertEquals("variant", copy.getTypeString());
  }
}
