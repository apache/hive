/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.MutableRecord;
import org.junit.Test;

public class TestBucketIdResolverImpl {

  private static final int TOTAL_BUCKETS = 12;
  private static final int RECORD_ID_COLUMN = 2;
  // id - TODO: use a non-zero index to check for offset errors.
  private static final int[] BUCKET_COLUMN_INDEXES = new int[] { 0 };

  private BucketIdResolver capturingBucketIdResolver = new BucketIdResolverImpl(
      ObjectInspectorFactory.getReflectionObjectInspector(MutableRecord.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA), RECORD_ID_COLUMN, TOTAL_BUCKETS, BUCKET_COLUMN_INDEXES);

  @Test
  public void testAttachBucketIdToRecord() {
    MutableRecord record = new MutableRecord(1, "hello");
    capturingBucketIdResolver.attachBucketIdToRecord(record);
    assertThat(record.rowId, is(new RecordIdentifier(-1L, 1, -1L)));
    assertThat(record.id, is(1));
    assertThat(record.msg.toString(), is("hello"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoBucketColumns() {
    new BucketIdResolverImpl(ObjectInspectorFactory.getReflectionObjectInspector(MutableRecord.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA), RECORD_ID_COLUMN, TOTAL_BUCKETS, new int[0]);

  }

}
