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

import static org.junit.Assume.assumeTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormats;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestHCatStorer extends AbstractHCatStorerTest {
  static Logger LOG = LoggerFactory.getLogger(TestHCatStorer.class);
  private static final Set<String> allTests = new HashSet<String>() {
    {
      add("testBagNStruct");
      add("testDateCharTypes");
      add("testDynamicPartitioningMultiPartColsInDataNoSpec");
      add("testDynamicPartitioningMultiPartColsInDataPartialSpec");
      add("testDynamicPartitioningMultiPartColsNoDataInDataNoSpec");
      add("testEmptyStore");
      add("testMultiPartColsInData");
      add("testNoAlias");
      add("testPartColsInData");
      add("testPartitionPublish");
      add("testStoreFuncAllSimpleTypes");
      add("testStoreFuncSimple");
      add("testStoreInPartiitonedTbl");
      add("testStoreMultiTables");
      add("testStoreWithNoCtorArgs");
      add("testStoreWithNoSchema");
      add("testWriteChar");
      add("testWriteDate");
      add("testWriteDate2");
      add("testWriteDate3");
      add("testWriteDecimal");
      add("testWriteDecimalX");
      add("testWriteDecimalXY");
      add("testWriteSmallint");
      add("testWriteTimestamp");
      add("testWriteTinyint");
      add("testWriteVarchar");
    }
  };

  /**
   * We're disabling these tests as they're going to be run from their individual
   * Test<FileFormat>HCatStorer classes. However, we're still leaving this test in case new file
   * formats in future are added.
   */
  private static final Map<String, Set<String>> DISABLED_STORAGE_FORMATS =
      new HashMap<String, Set<String>>() {
        {
          put(IOConstants.AVRO, allTests);
          put(IOConstants.ORCFILE, allTests);
          put(IOConstants.PARQUETFILE, allTests);
          put(IOConstants.RCFILE, allTests);
          put(IOConstants.SEQUENCEFILE, allTests);
          put(IOConstants.TEXTFILE, allTests);
        }
      };

  private String storageFormat;

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return StorageFormats.names();
  }

  public TestHCatStorer(String storageFormat) {
    this.storageFormat = storageFormat;
  }

  @Override
  String getStorageFormat() {
    return null;
  }

  @Test
  @Override
  public void testWriteTinyint() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteTinyint();
  }

  @Test
  @Override
  public void testWriteSmallint() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteSmallint();
  }

  @Test
  @Override
  public void testWriteChar() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteChar();
  }

  @Test
  @Override
  public void testWriteVarchar() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteVarchar();
  }

  @Test
  @Override
  public void testWriteDecimalXY() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDecimalXY();
  }

  @Test
  @Override
  public void testWriteDecimalX() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDecimalX();
  }

  @Test
  @Override
  public void testWriteDecimal() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDecimal();
  }

  @Test
  @Override
  public void testWriteDate() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDate();
  }

  @Test
  @Override
  public void testWriteDate3() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDate3();
  }

  @Test
  @Override
  public void testWriteDate2() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteDate2();
  }

  @Test
  @Override
  public void testWriteTimestamp() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testWriteTimestamp();
  }

  @Test
  @Override
  public void testDateCharTypes() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testDateCharTypes();
  }

  @Test
  @Override
  public void testPartColsInData() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testPartColsInData();
  }

  @Test
  @Override
  public void testMultiPartColsInData() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testMultiPartColsInData();
  }

  @Test
  @Override
  public void testStoreInPartiitonedTbl() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreInPartiitonedTbl();
  }

  @Test
  @Override
  public void testNoAlias() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testNoAlias();
  }

  @Test
  @Override
  public void testStoreMultiTables() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreMultiTables();
  }

  @Test
  @Override
  public void testStoreWithNoSchema() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreWithNoSchema();
  }

  @Test
  @Override
  public void testStoreWithNoCtorArgs() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreWithNoCtorArgs();
  }

  @Test
  @Override
  public void testEmptyStore() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testEmptyStore();
  }

  @Test
  @Override
  public void testBagNStruct() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testBagNStruct();
  }

  @Test
  @Override
  public void testStoreFuncAllSimpleTypes() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreFuncAllSimpleTypes();
  }

  @Test
  @Override
  public void testStoreFuncSimple() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testStoreFuncSimple();
  }

  @Test
  @Override
  public void testDynamicPartitioningMultiPartColsInDataPartialSpec() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testDynamicPartitioningMultiPartColsInDataPartialSpec();
  }

  @Test
  @Override
  public void testDynamicPartitioningMultiPartColsInDataNoSpec() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testDynamicPartitioningMultiPartColsInDataNoSpec();
  }

  @Test
  @Override
  public void testDynamicPartitioningMultiPartColsNoDataInDataNoSpec() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testDynamicPartitioningMultiPartColsNoDataInDataNoSpec();
  }

  @Test
  @Override
  public void testPartitionPublish() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    super.testPartitionPublish();
  }
}
