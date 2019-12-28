/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.predicate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestPrimitiveComparisonFilter {

  @Test
  public void testBase64ConstantEncode() {
    PrimitiveComparisonFilter filter = new PrimitiveComparisonFilter();
    Map<String,String> options = new HashMap<String,String>();

    for (int i = 0; i < 500; i++) {
      String constant = Integer.toString(i);
      options.put(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(constant.getBytes()));

      Assert.assertEquals(constant, new String(filter.getConstant(options)));
    }
  }

  @Test
  public void testNumericBase64ConstantEncode() throws IOException {
    PrimitiveComparisonFilter filter = new PrimitiveComparisonFilter();
    Map<String,String> options = new HashMap<String,String>();
    IntWritable writable = new IntWritable();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    for (int i = 0; i < 500; i++) {
      writable.set(i);
      writable.write(out);

      options.put(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(baos.toByteArray()));

      byte[] bytes = filter.getConstant(options);

      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream in = new DataInputStream(bais);
      writable.readFields(in);

      Assert.assertEquals(i, writable.get());

      baos.reset();
    }
  }
}
