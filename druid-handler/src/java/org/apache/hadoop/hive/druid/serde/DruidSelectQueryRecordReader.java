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
package org.apache.hadoop.hive.druid.serde;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterators;

import io.druid.query.Result;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;

/**
 * Record reader for results for Druid SelectQuery.
 */
public class DruidSelectQueryRecordReader
        extends DruidQueryRecordReader<SelectQuery, Result<SelectResultValue>> {

  private Result<SelectResultValue> current;

  private Iterator<EventHolder> values = Iterators.emptyIterator();

  @Override
  protected SelectQuery createQuery(String content) throws IOException {
    return DruidStorageHandlerUtils.JSON_MAPPER.readValue(content, SelectQuery.class);
  }

  @Override
  protected List<Result<SelectResultValue>> createResultsList(InputStream content)
          throws IOException {
    return DruidStorageHandlerUtils.SMILE_MAPPER.readValue(content,
            new TypeReference<List<Result<SelectResultValue>>>() {
            }
    );
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (values.hasNext()) {
      return true;
    }
    if (results.hasNext()) {
      current = results.next();
      values = current.getValue().getEvents().iterator();
      return nextKeyValue();
    }
    return false;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public DruidWritable getCurrentValue() throws IOException, InterruptedException {
    // Create new value
    DruidWritable value = new DruidWritable();
    EventHolder e = values.next();
    value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, e.getTimestamp().getMillis());
    value.getValue().putAll(e.getEvent());
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) throws IOException {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      EventHolder e = values.next();
      value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, e.getTimestamp().getMillis());
      value.getValue().putAll(e.getEvent());
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() {
    return results.hasNext() || values.hasNext() ? 0 : 1;
  }

}
