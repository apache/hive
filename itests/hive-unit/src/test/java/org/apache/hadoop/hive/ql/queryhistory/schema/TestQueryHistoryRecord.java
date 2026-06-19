/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.hadoop.hive.ql.queryhistory.schema;

import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.Assert;
import org.junit.Test;

public class TestQueryHistoryRecord {

  private static final int EMPTY_RECORD_ESTIMATED_SIZE_BYTES = 1368;

  @Test
  public void testEmptyRecord() {
    Record record = new Record();
    // some fields are initialized right after constructing the object
    Assert.assertEquals(record.get(Schema.Field.QUERY_HISTORY_SCHEMA_VERSION),
        Schema.CURRENT_VERSION);
    Assert.assertEquals(record.get(Schema.Field.HIVE_VERSION), HiveVersionInfo.getVersion());
  }

  /**
   * This test is crucial for ensuring that unit tests using DummyRecord have coverage on all fields.
   */
  @Test
  public void testExampleRecordIsFullySet() {
    Record record = new DummyRecord();
    for (Schema.Field field : Schema.Field.values()) {
      Assert.assertNotNull("Field should be filled with example value: " + field.getName(), record.get(field));
    }
  }

  @Test
  public void testBasicRecordEstimatedSizes() {
    Record emptyRecord = new Record();
    Record exampleRecord = new DummyRecord();
    // these assertions have no strict meaning, they are just for demonstrating the current estimated size of a query
    // history record
    Assert.assertEquals(EMPTY_RECORD_ESTIMATED_SIZE_BYTES, emptyRecord.getEstimatedSizeInMemoryBytes());
    Assert.assertEquals(1526, exampleRecord.getEstimatedSizeInMemoryBytes());
  }

  @Test
  public void testRecordSizeIsNotRecalculatedAfterFirstCall() {
    Record emptyRecord = new Record();
    Assert.assertEquals(EMPTY_RECORD_ESTIMATED_SIZE_BYTES, emptyRecord.getEstimatedSizeInMemoryBytes());

    emptyRecord.setPlan("asdfghjk");
    // size is not changed, current implementation strictly calculates it only once after adding it to the queue of
    // records to be persisted
    Assert.assertEquals(EMPTY_RECORD_ESTIMATED_SIZE_BYTES, emptyRecord.getEstimatedSizeInMemoryBytes());
  }

  @Test
  public void testRecordSizeWithChangedPlan() {
    Record emptyRecord = new Record();
    // don't call getEstimatedSizeInMemoryBytes here, we know it's EMPTY_RECORD_ESTIMATED_SIZE_BYTES
    String plan = "asdfghjk";
    emptyRecord.setPlan(plan);
    // size is changed
    Assert.assertEquals(EMPTY_RECORD_ESTIMATED_SIZE_BYTES + plan.length() * Character.BYTES,
        emptyRecord.getEstimatedSizeInMemoryBytes());
  }
}
