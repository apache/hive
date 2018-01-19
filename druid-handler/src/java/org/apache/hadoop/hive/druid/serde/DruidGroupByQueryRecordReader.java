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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.http.client.HttpClient;
import io.druid.data.input.MapBasedRow;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.fasterxml.jackson.core.type.TypeReference;

import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import org.joda.time.format.ISODateTimeFormat;

import static org.apache.hadoop.hive.druid.serde.DruidSerDeUtils.ISO_TIME_FORMAT;

/**
 * Record reader for results for Druid GroupByQuery.
 */
public class DruidGroupByQueryRecordReader
        extends DruidQueryRecordReader<GroupByQuery, Row> {
  private final static TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>() {
  };

  private MapBasedRow currentRow;
  private Map<String, Object> currentEvent;

  private List<String> timeExtractionFields = Lists.newArrayList();
  private List<String> intFormattedTimeExtractionFields = Lists.newArrayList();

  @Override
  public void initialize(InputSplit split, Configuration conf) throws IOException {
    super.initialize(split, conf);
    initDimensionTypes();
  }

  @Override
  public void initialize(InputSplit split, Configuration conf, ObjectMapper mapper,
          ObjectMapper smileMapper, HttpClient httpClient
  ) throws IOException {
    super.initialize(split, conf, mapper, smileMapper, httpClient);
    initDimensionTypes();
  }

  @Override
  protected JavaType getResultTypeDef() {
    return DruidStorageHandlerUtils.JSON_MAPPER.getTypeFactory().constructType(TYPE_REFERENCE);
  }

  private void initDimensionTypes() throws IOException {
    //@TODO move this out of here to org.apache.hadoop.hive.druid.serde.DruidSerDe
    List<DimensionSpec> dimensionSpecList = ((GroupByQuery) query).getDimensions();
    List<DimensionSpec> extractionDimensionSpecList = dimensionSpecList.stream()
            .filter(dimensionSpecs -> dimensionSpecs instanceof ExtractionDimensionSpec)
            .collect(Collectors.toList());
    extractionDimensionSpecList.stream().forEach(dimensionSpec -> {
      ExtractionDimensionSpec extractionDimensionSpec = (ExtractionDimensionSpec) dimensionSpec;
      if (extractionDimensionSpec.getExtractionFn() instanceof TimeFormatExtractionFn) {
        final TimeFormatExtractionFn timeFormatExtractionFn = (TimeFormatExtractionFn) extractionDimensionSpec
                .getExtractionFn();
        if (timeFormatExtractionFn  == null || timeFormatExtractionFn.getFormat().equals(ISO_TIME_FORMAT)) {
          timeExtractionFields.add(extractionDimensionSpec.getOutputName());
        } else {
          intFormattedTimeExtractionFields.add(extractionDimensionSpec.getOutputName());
        }
      }
    });
  }

  @Override
  public boolean nextKeyValue() {
    // Results

    if (queryResultsIterator.hasNext()) {
      final Row row = queryResultsIterator.next();
      // currently druid supports only MapBasedRow as Jackson SerDe so it should safe to cast without check
      currentRow = (MapBasedRow) row;
      //@TODO move this out of here to org.apache.hadoop.hive.druid.serde.DruidSerDe
      currentEvent = Maps.transformEntries(currentRow.getEvent(),
              (key, value1) -> {
                if (timeExtractionFields.contains(key)) {
                  return ISODateTimeFormat.dateTimeParser().parseMillis((String) value1);
                }
                if (intFormattedTimeExtractionFields.contains(key)) {
                  return Integer.valueOf((String) value1);
                }
                return value1;
              }
      );
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
    value.getValue().put(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN, currentRow.getTimestamp().getMillis());
    // 2) The dimension columns
    value.getValue().putAll(currentEvent);
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      // 1) The timestamp column
      value.getValue().put(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN, currentRow.getTimestamp().getMillis());
      // 2) The dimension columns
      value.getValue().putAll(currentEvent);
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException {
    return queryResultsIterator.hasNext() ? 0 : 1;
  }

}
