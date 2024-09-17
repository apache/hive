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

import org.apache.iceberg.data.GenericRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;

/**
 * Convenient class for creating a QueryHistoryRecord from an iceberg Container.
 */
public class IcebergQueryHistoryRecord extends QueryHistoryRecord {

  public IcebergQueryHistoryRecord(GenericRecord icebergRecord) {
    for (QueryHistorySchema.Field field : QueryHistorySchema.Field.values()) {
      record.put(field.getName(), icebergRecord.getField(field.getName()));
    }
  }
}
