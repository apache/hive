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
package org.apache.hadoop.hive.druid.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Map;

/**
 * Record reader for results for Druid GroupByQuery.
 */
public class DruidGroupByQueryRecordReader extends DruidQueryRecordReader<Row> {
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>() {
  };

  private MapBasedRow currentRow;
  private Map<String, Object> currentEvent;

  @Override protected JavaType getResultTypeDef() {
    return DruidStorageHandlerUtils.JSON_MAPPER.getTypeFactory().constructType(TYPE_REFERENCE);
  }

  @Override public boolean nextKeyValue() {
    // Results

    if (getQueryResultsIterator().hasNext()) {
      final Row row = getQueryResultsIterator().next();
      // currently druid supports only MapBasedRow as Jackson SerDe so it should safe to cast without check
      currentRow = (MapBasedRow) row;
      currentEvent = currentRow.getEvent();
      return true;
    }
    return false;
  }

  @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override public DruidWritable getCurrentValue() throws IOException, InterruptedException {
    // Create new value
    DruidWritable value = new DruidWritable(false);
    // 1) The timestamp column
    value.getValue().put(DruidConstants.EVENT_TIMESTAMP_COLUMN,
        currentRow.getTimestamp() == null ? null : currentRow.getTimestamp().getMillis()
    );
    // 2) The dimension columns
    value.getValue().putAll(currentEvent);
    return value;
  }

  @Override public boolean next(NullWritable key, DruidWritable value) {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      // 1) The timestamp column
      value.getValue().put(DruidConstants.EVENT_TIMESTAMP_COLUMN,
          currentRow.getTimestamp() == null ? null : currentRow.getTimestamp().getMillis()
      );
      // 2) The dimension columns
      value.getValue().putAll(currentEvent);
      return true;
    }
    return false;
  }

  @Override public float getProgress() throws IOException {
    return getQueryResultsIterator().hasNext() ? 0 : 1;
  }

}
