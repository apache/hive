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
import java.util.List;

import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.fasterxml.jackson.core.type.TypeReference;

import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;

/**
 * Record reader for results for Druid GroupByQuery.
 */
public class DruidGroupByQueryRecordReader
        extends DruidQueryRecordReader<GroupByQuery, Row> {

  private Row current;

  private int[] indexes = new int[0];

  // Row objects returned by GroupByQuery have different access paths depending on
  // whether the result for the metric is a Float or a Long, thus we keep track
  // using these converters
  private Extract[] extractors;

  @Override
  public void initialize(InputSplit split, Configuration conf) throws IOException {
    super.initialize(split, conf);
    initExtractors();
  }

  @Override
  protected GroupByQuery createQuery(String content) throws IOException {
    return DruidStorageHandlerUtils.JSON_MAPPER.readValue(content, GroupByQuery.class);
  }

  @Override
  protected List<Row> createResultsList(InputStream content) throws IOException {
    return DruidStorageHandlerUtils.SMILE_MAPPER.readValue(content,
            new TypeReference<List<Row>>() {
            }
    );
  }

  private void initExtractors() throws IOException {
    extractors = new Extract[query.getAggregatorSpecs().size() + query.getPostAggregatorSpecs()
            .size()];
    int counter = 0;
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++, counter++) {
      AggregatorFactory af = query.getAggregatorSpecs().get(i);
      switch (af.getTypeName().toUpperCase()) {
        case DruidSerDeUtils.FLOAT_TYPE:
          extractors[counter] = Extract.FLOAT;
          break;
        case DruidSerDeUtils.LONG_TYPE:
          extractors[counter] = Extract.LONG;
          break;
        default:
          throw new IOException("Type not supported");
      }
    }
    for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++, counter++) {
      extractors[counter] = Extract.FLOAT;
    }
  }

  @Override
  public boolean nextKeyValue() {
    // Refresh indexes
    for (int i = indexes.length - 1; i >= 0; i--) {
      if (indexes[i] > 0) {
        indexes[i]--;
        for (int j = i + 1; j < indexes.length; j++) {
          indexes[j] = current.getDimension(
                  query.getDimensions().get(j).getDimension()).size() - 1;
        }
        return true;
      }
    }
    // Results
    if (results.hasNext()) {
      current = results.next();
      indexes = new int[query.getDimensions().size()];
      for (int i = 0; i < query.getDimensions().size(); i++) {
        DimensionSpec ds = query.getDimensions().get(i);
        indexes[i] = current.getDimension(ds.getDimension()).size() - 1;
      }
      return true;
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
    // 1) The timestamp column
    value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, current.getTimestamp().getMillis());
    // 2) The dimension columns
    for (int i = 0; i < query.getDimensions().size(); i++) {
      DimensionSpec ds = query.getDimensions().get(i);
      List<String> dims = current.getDimension(ds.getDimension());
      if (dims.size() == 0) {
        // NULL value for dimension
        value.getValue().put(ds.getOutputName(), null);
      } else {
        int pos = dims.size() - indexes[i] - 1;
        value.getValue().put(ds.getOutputName(), dims.get(pos));
      }
    }
    int counter = 0;
    // 3) The aggregation columns
    for (AggregatorFactory af : query.getAggregatorSpecs()) {
      switch (extractors[counter++]) {
        case FLOAT:
          value.getValue().put(af.getName(), current.getFloatMetric(af.getName()));
          break;
        case LONG:
          value.getValue().put(af.getName(), current.getLongMetric(af.getName()));
          break;
      }
    }
    // 4) The post-aggregation columns
    for (PostAggregator pa : query.getPostAggregatorSpecs()) {
      assert extractors[counter++] == Extract.FLOAT;
      value.getValue().put(pa.getName(), current.getFloatMetric(pa.getName()));
    }
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      // 1) The timestamp column
      value.getValue().put(DruidTable.DEFAULT_TIMESTAMP_COLUMN, current.getTimestamp().getMillis());
      // 2) The dimension columns
      for (int i = 0; i < query.getDimensions().size(); i++) {
        DimensionSpec ds = query.getDimensions().get(i);
        List<String> dims = current.getDimension(ds.getDimension());
        if (dims.size() == 0) {
          // NULL value for dimension
          value.getValue().put(ds.getOutputName(), null);
        } else {
          int pos = dims.size() - indexes[i] - 1;
          value.getValue().put(ds.getOutputName(), dims.get(pos));
        }
      }
      int counter = 0;
      // 3) The aggregation columns
      for (AggregatorFactory af : query.getAggregatorSpecs()) {
        switch (extractors[counter++]) {
          case FLOAT:
            value.getValue().put(af.getName(), current.getFloatMetric(af.getName()));
            break;
          case LONG:
            value.getValue().put(af.getName(), current.getLongMetric(af.getName()));
            break;
        }
      }
      // 4) The post-aggregation columns
      for (PostAggregator pa : query.getPostAggregatorSpecs()) {
        assert extractors[counter++] == Extract.FLOAT;
        value.getValue().put(pa.getName(), current.getFloatMetric(pa.getName()));
      }
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException {
    return results.hasNext() ? 0 : 1;
  }

  private enum Extract {
    FLOAT,
    LONG
  }

}
