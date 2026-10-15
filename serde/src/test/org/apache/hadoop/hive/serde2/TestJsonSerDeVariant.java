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
package org.apache.hadoop.hive.serde2;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.columns;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.row;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variantOI;
import static org.junit.Assert.assertEquals;

public class TestJsonSerDeVariant {

  @Test
  public void testTextIsTheObjectOfTheTwoBase64Binaries() throws Exception {
    Variant a = variant("{\"k\":1}");
    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), columns("v,l", "variant,variant"), null, false);
    Text text = (Text) serde.serialize(Arrays.asList(a, null),
        row(List.of("v", "l"), List.of(variantOI(), variantOI())));
    assertEquals("{\"v\":{\"metadata\":\"" + Base64.getEncoder().encodeToString(a.getMetadata())
        + "\",\"value\":\"" + Base64.getEncoder().encodeToString(a.getValue()) + "\"},\"l\":null}", text.toString());
  }
}
