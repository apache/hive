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
package org.apache.hive.hcatalog.pig;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroHCatStorer extends AbstractHCatStorerTest {
  static Logger LOG = LoggerFactory.getLogger(TestAvroHCatStorer.class);

  @Override
  String getStorageFormat() {
    return IOConstants.AVRO;
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  // incorrect precision: expected:<0 xxxxx yyy 5.2[]> but was:<0 xxxxx yyy 5.2[0]>
  public void testDateCharTypes() throws Exception {
    super.testDateCharTypes();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  // incorrect precision: expected:<1.2[]> but was:<1.2[0]>
  public void testWriteDecimalXY() throws Exception {
    super.testWriteDecimalXY();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  // doesn't have a notion of small, and saves the full value as an int, so no overflow
  // expected:<null> but was:<32768>
  public void testWriteSmallint() throws Exception {
    super.testWriteSmallint();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  // does not support timestamp
  // TypeInfoToSchema.createAvroPrimitive : UnsupportedOperationException
  public void testWriteTimestamp() throws Exception {
    super.testWriteTimestamp();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  // doesn't have a notion of tiny, and saves the full value as an int, so no overflow
  // expected:<null> but was:<300>
  public void testWriteTinyint() throws Exception {
    super.testWriteTinyint();
  }
}
