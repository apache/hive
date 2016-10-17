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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.pig;

import java.io.IOException;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetHCatStorer extends AbstractHCatStorerTest {
  static Logger LOG = LoggerFactory.getLogger(TestParquetHCatStorer.class);

  @Override
  String getStorageFormat() {
    return IOConstants.PARQUETFILE;
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testBagNStruct() throws IOException, CommandNeedRetryException {
    super.testBagNStruct();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testDateCharTypes() throws Exception {
    super.testDateCharTypes();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testDynamicPartitioningMultiPartColsInDataNoSpec() throws IOException,
      CommandNeedRetryException {
    super.testDynamicPartitioningMultiPartColsInDataNoSpec();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testDynamicPartitioningMultiPartColsInDataPartialSpec() throws IOException,
      CommandNeedRetryException {
    super.testDynamicPartitioningMultiPartColsInDataPartialSpec();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testMultiPartColsInData() throws Exception {
    super.testMultiPartColsInData();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testPartColsInData() throws IOException, CommandNeedRetryException {
    super.testPartColsInData();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreFuncAllSimpleTypes() throws IOException, CommandNeedRetryException {
    super.testStoreFuncAllSimpleTypes();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreFuncSimple() throws IOException, CommandNeedRetryException {
    super.testStoreFuncSimple();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreInPartiitonedTbl() throws Exception {
    super.testStoreInPartiitonedTbl();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreMultiTables() throws IOException, CommandNeedRetryException {
    super.testStoreMultiTables();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreWithNoCtorArgs() throws IOException, CommandNeedRetryException {
    super.testStoreWithNoCtorArgs();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testStoreWithNoSchema() throws IOException, CommandNeedRetryException {
    super.testStoreWithNoSchema();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteChar() throws Exception {
    super.testWriteChar();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDate() throws Exception {
    super.testWriteDate();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDate2() throws Exception {
    super.testWriteDate2();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDate3() throws Exception {
    super.testWriteDate3();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDecimal() throws Exception {
    super.testWriteDecimal();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDecimalX() throws Exception {
    super.testWriteDecimalX();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteDecimalXY() throws Exception {
    super.testWriteDecimalXY();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteSmallint() throws Exception {
    super.testWriteSmallint();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteTimestamp() throws Exception {
    super.testWriteTimestamp();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteTinyint() throws Exception {
    super.testWriteTinyint();
  }

  @Test
  @Override
  @Ignore("Temporarily disable until fixed")
  public void testWriteVarchar() throws Exception {
    super.testWriteVarchar();
  }
}
