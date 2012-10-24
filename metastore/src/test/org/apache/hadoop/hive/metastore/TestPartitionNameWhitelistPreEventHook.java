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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.junit.Test;

// Validate CharacterWhitelistPreEventHook to ensure it refuses to process
// a partition add or append request if partition fields contain
// Unicode characters or commas

public class TestPartitionNameWhitelistPreEventHook {

  // Runs an instance of DisallowUnicodePreEventListener
  // Returns whether or not it succeeded
  private boolean runHook(PreEventContext event) {

    Configuration config = new Configuration();

    // match the printable ASCII characters except for commas
    config.set(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname
        , "[\\x20-\\x7E&&[^,]]*");

    PartitionNameWhitelistPreEventListener hook =
      new PartitionNameWhitelistPreEventListener(config);

    try {
      hook.onEvent(event);
    } catch (Exception e) {
      return false;
    }

    return true;
 }

  // Sample data
  private List<String> getPartValsWithUnicode() {

    List<String> partVals = new ArrayList<String>();
    partVals.add("klâwen");
    partVals.add("tägelîch");

    return partVals;

  }

  private List<String> getPartValsWithCommas() {

    List<String> partVals = new ArrayList<String>();
    partVals.add("a,b");
    partVals.add("c,d,e,f");

    return partVals;

  }

  private List<String> getPartValsWithValidCharacters() {

    List<String> partVals = new ArrayList<String>();
    partVals.add("part1");
    partVals.add("part2");

    return partVals;

  }

  @Test
  public void testAddPartitionWithCommas() {

    Partition partition = new Partition();
    partition.setValues(getPartValsWithCommas());

    PreAddPartitionEvent event = new PreAddPartitionEvent(partition, null);

    Assert.assertFalse("Add a partition with commas in name",
                       runHook(event));
  }

  @Test
  public void testAddPartitionWithUnicode() {

    Partition partition = new Partition();
    partition.setValues(getPartValsWithUnicode());

    PreAddPartitionEvent event = new PreAddPartitionEvent(partition, null);

    Assert.assertFalse("Add a partition with unicode characters in name",
                       runHook(event));
  }

  @Test
  public void testAddPartitionWithValidPartVal() {

    Partition p = new Partition();
    p.setValues(getPartValsWithValidCharacters());

    PreAddPartitionEvent event = new PreAddPartitionEvent(p, null);

    Assert.assertTrue("Add a partition with unicode characters in name",
                       runHook(event));
  }

  @Test
  public void testAppendPartitionWithUnicode() {

    Partition p = new Partition();
    p.setValues(getPartValsWithUnicode());

    PreAddPartitionEvent event = new PreAddPartitionEvent(p, null);

    Assert.assertFalse("Append a partition with unicode characters in name",
                       runHook(event));
  }

  @Test
  public void testAppendPartitionWithCommas() {

    Partition p = new Partition();
    p.setValues(getPartValsWithCommas());

    PreAddPartitionEvent event = new PreAddPartitionEvent(p, null);

    Assert.assertFalse("Append a partition with unicode characters in name",
                       runHook(event));
  }

  @Test
  public void testAppendPartitionWithValidCharacters() {

    Partition p = new Partition();
    p.setValues(getPartValsWithValidCharacters());

    PreAddPartitionEvent event = new PreAddPartitionEvent(p, null);

    Assert.assertTrue("Append a partition with no unicode characters in name",
                       runHook(event));
  }

}
