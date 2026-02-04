/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

public final class RowLineageReader {

  private RowLineageReader() {
  }

  public static Long readRowId(Record rec) {
    return (Long) rec.getField(MetadataColumns.ROW_ID.name());
  }

  public static Long readLastUpdatedSequenceNumber(Record rec) {
    return (Long) rec.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
  }

  public static boolean schemaHasRowLineageColumns(Schema schema) {
    return schema.findField(MetadataColumns.ROW_ID.name()) != null ||
        schema.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()) != null;
  }
}
