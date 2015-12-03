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
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Standard {@link RecordInspector} implementation that uses the supplied {@link ObjectInspector} and
 * {@link AcidOutputFormat.Options#recordIdColumn(int) record id column} to extract {@link RecordIdentifier
 * RecordIdentifiers}, and calculate bucket ids from records.
 */
public class RecordInspectorImpl implements RecordInspector {

  private final StructObjectInspector structObjectInspector;
  private final StructField recordIdentifierField;

  /**
   * Note that all column indexes are with respect to your record structure, not the Hive table structure.
   */
  public RecordInspectorImpl(ObjectInspector objectInspector, int recordIdColumn) {
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new IllegalArgumentException("Serious problem, expected a StructObjectInspector, " + "but got a "
          + objectInspector.getClass().getName());
    }

    structObjectInspector = (StructObjectInspector) objectInspector;
    List<? extends StructField> structFields = structObjectInspector.getAllStructFieldRefs();
    recordIdentifierField = structFields.get(recordIdColumn);
  }

  public RecordIdentifier extractRecordIdentifier(Object record) {
    return (RecordIdentifier) structObjectInspector.getStructFieldData(record, recordIdentifierField);
  }

  @Override
  public String toString() {
    return "RecordInspectorImpl [structObjectInspector=" + structObjectInspector + ", recordIdentifierField="
        + recordIdentifierField + "]";
  }

}
