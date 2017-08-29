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

import java.util.List;

import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * Implementation of a {@link BucketIdResolver} that includes the logic required to calculate a bucket id from a record
 * that is consistent with Hive's own internal computation scheme.
 */
public class BucketIdResolverImpl implements BucketIdResolver {

  private static final long INVALID_TRANSACTION_ID = -1L;
  private static final long INVALID_ROW_ID = -1L;

  private final SettableStructObjectInspector structObjectInspector;
  private final StructField[] bucketFields;
  private final int totalBuckets;
  private final StructField recordIdentifierField;

  /**
   * Note that all column indexes are with respect to your record structure, not the Hive table structure. Bucket column
   * indexes must be presented in the same order as they are in the Hive table definition.
   */
  public BucketIdResolverImpl(ObjectInspector objectInspector, int recordIdColumn, int totalBuckets, int[] bucketColumns) {
    this.totalBuckets = totalBuckets;
    if (!(objectInspector instanceof SettableStructObjectInspector)) {
      throw new IllegalArgumentException("Serious problem, expected a StructObjectInspector, " + "but got a "
          + objectInspector.getClass().getName());
    }

    if (bucketColumns.length < 1) {
      throw new IllegalArgumentException("No bucket column indexes set.");
    }
    structObjectInspector = (SettableStructObjectInspector) objectInspector;
    List<? extends StructField> structFields = structObjectInspector.getAllStructFieldRefs();

    recordIdentifierField = structFields.get(recordIdColumn);

    bucketFields = new StructField[bucketColumns.length];
    for (int i = 0; i < bucketColumns.length; i++) {
      int bucketColumnsIndex = bucketColumns[i];
      bucketFields[i] = structFields.get(bucketColumnsIndex);
    }
  }

  @Override
  public Object attachBucketIdToRecord(Object record) {
    int bucketId = computeBucketId(record);
    int bucketProperty =
      BucketCodec.V1.encode(new AcidOutputFormat.Options(null).bucket(bucketId));
    RecordIdentifier recordIdentifier = new RecordIdentifier(INVALID_TRANSACTION_ID, bucketProperty, INVALID_ROW_ID);
    structObjectInspector.setStructFieldData(record, recordIdentifierField, recordIdentifier);
    return record;
  }

  @Override
  public int computeBucketId(Object record) {
    Object[] bucketFieldValues = new Object[bucketFields.length];
    ObjectInspector[] bucketFiledInspectors = new ObjectInspector[bucketFields.length];
    for (int columnIndex = 0; columnIndex < bucketFields.length; columnIndex++) {
      bucketFieldValues[columnIndex] = structObjectInspector.getStructFieldData(record, bucketFields[columnIndex]);
      bucketFiledInspectors[columnIndex] = bucketFields[columnIndex].getFieldObjectInspector();
    }
    return ObjectInspectorUtils.getBucketNumber(bucketFieldValues, bucketFiledInspectors, totalBuckets);
  }

}
