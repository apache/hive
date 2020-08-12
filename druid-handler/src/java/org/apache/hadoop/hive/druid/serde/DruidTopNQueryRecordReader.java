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
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;

/**
 * Record reader for results for Druid TopNQuery.
 */
public class DruidTopNQueryRecordReader
        extends DruidQueryRecordReader<TopNQuery, Result<TopNResultValue>> {

  private Result<TopNResultValue> current;

  private Iterator<DimensionAndMetricValueExtractor> values = Iterators.emptyIterator();

  @Override
  protected TopNQuery createQuery(String content) throws IOException {
    return DruidStorageHandlerUtils.JSON_MAPPER.readValue(content, TopNQuery.class);
  }

  @Override
  protected List<Result<TopNResultValue>> createResultsList(InputStream content)
          throws IOException {
    return DruidStorageHandlerUtils.SMILE_MAPPER.readValue(content,
            new TypeReference<List<Result<TopNResultValue>>>() {
            }
    );
  }

  @Override
  public boolean nextKeyValue() {
    if (values.hasNext()) {
      return true;
    }
    if (results.hasNext()) {
      current = results.next();
      values = current.getValue().getValue().iterator();
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
    value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, current.getTimestamp().getMillis());
    if (values.hasNext()) {
      value.getValue().putAll(values.next().getBaseObject());
      return value;
    }
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, current.getTimestamp().getMillis());
      if (values.hasNext()) {
        value.getValue().putAll(values.next().getBaseObject());
      }
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() {
    return results.hasNext() || values.hasNext() ? 0 : 1;
  }

}
