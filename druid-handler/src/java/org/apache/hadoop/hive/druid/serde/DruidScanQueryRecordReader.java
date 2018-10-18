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

import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanResultValue;

import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Record reader for results for Druid ScanQuery.
 */
public class DruidScanQueryRecordReader
    extends DruidQueryRecordReader<ScanQuery, ScanResultValue> {

  private static final TypeReference<ScanResultValue> TYPE_REFERENCE =
      new TypeReference<ScanResultValue>() {
      };

  private ScanResultValue current;

  private Iterator<List<Object>> compactedValues = Iterators.emptyIterator();

  @Override
  protected JavaType getResultTypeDef() {
    return DruidStorageHandlerUtils.JSON_MAPPER.getTypeFactory().constructType(TYPE_REFERENCE);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (compactedValues.hasNext()) {
      return true;
    }
    if (queryResultsIterator.hasNext()) {
      current = queryResultsIterator.next();
      compactedValues = ((List<List<Object>>) current.getEvents()).iterator();
      return nextKeyValue();
    }
    return false;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public DruidWritable createValue()
  {
    return new DruidWritable(true);
  }

  @Override
  public DruidWritable getCurrentValue() throws IOException, InterruptedException {
    // Create new value
    DruidWritable value = new DruidWritable(true);
    value.setCompactedValue(compactedValues.next());
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) throws IOException {
    if (nextKeyValue()) {
      // Update value
      value.setCompactedValue(compactedValues.next());
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() {
    return queryResultsIterator.hasNext() || compactedValues.hasNext() ? 0 : 1;
  }

}
