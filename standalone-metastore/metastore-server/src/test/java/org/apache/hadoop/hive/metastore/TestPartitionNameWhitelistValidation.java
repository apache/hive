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

package org.apache.hadoop.hive.metastore;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Validate the metastore client call validatePartitionNameCharacters to ensure it throws
// an exception if partition fields contain Unicode characters or commas

@Category(MetastoreUnitTest.class)
public class TestPartitionNameWhitelistValidation {

  private static final String partitionValidationPattern = "[\\x20-\\x7E&&[^,]]*";
  private static Configuration conf;
  private static HiveMetaStoreClient msc;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    System.setProperty(ConfVars.PARTITION_NAME_WHITELIST_PATTERN.toString(), partitionValidationPattern);
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    msc = new HiveMetaStoreClient(conf);
  }

  // Runs an instance of DisallowUnicodePreEventListener
  // Returns whether or not it succeeded
  private boolean runValidation(List<String> partVals) {
    try {
      msc.validatePartitionNameCharacters(partVals);
    } catch (Exception e) {
      return false;
    }

    return true;
 }

  // Sample data
  private List<String> getPartValsWithUnicode() {
    List<String> partVals = new ArrayList<>();
    partVals.add("klâwen");
    partVals.add("tägelîch");

    return partVals;
  }

  private List<String> getPartValsWithCommas() {
    List<String> partVals = new ArrayList<>();
    partVals.add("a,b");
    partVals.add("c,d,e,f");

    return partVals;
  }

  private List<String> getPartValsWithValidCharacters() {
    List<String> partVals = new ArrayList<>();
    partVals.add("part1");
    partVals.add("part2");

    return partVals;
  }

  @Test
  public void testAddPartitionWithCommas() {
    assertFalse("Add a partition with commas in name",
        runValidation(getPartValsWithCommas()));
  }

  @Test
  public void testAddPartitionWithUnicode() {
    assertFalse("Add a partition with unicode characters in name",
        runValidation(getPartValsWithUnicode()));
  }

  @Test
  public void testAddPartitionWithValidPartVal() {
    assertTrue("Add a partition with unicode characters in name",
        runValidation(getPartValsWithValidCharacters()));
  }

  @Test
  public void testAppendPartitionWithUnicode() {
    assertFalse("Append a partition with unicode characters in name",
        runValidation(getPartValsWithUnicode()));
  }

  @Test
  public void testAppendPartitionWithCommas() {
    assertFalse("Append a partition with unicode characters in name",
        runValidation(getPartValsWithCommas()));
  }

  @Test
  public void testAppendPartitionWithValidCharacters() {
    assertTrue("Append a partition with no unicode characters in name",
        runValidation(getPartValsWithValidCharacters()));
  }

}
