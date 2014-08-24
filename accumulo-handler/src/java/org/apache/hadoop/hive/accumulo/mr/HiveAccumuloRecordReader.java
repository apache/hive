/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import com.google.common.collect.Lists;

/**
 * Translate the {@link Key} {@link Value} pairs from {@link AccumuloInputFormat} to a
 * {@link Writable} for consumption by the {@link AccumuloSerDe}.
 */
public class HiveAccumuloRecordReader implements RecordReader<Text,AccumuloHiveRow> {
  private RecordReader<Text,PeekingIterator<Entry<Key,Value>>> recordReader;
  private int iteratorCount;

  public HiveAccumuloRecordReader(
      RecordReader<Text,PeekingIterator<Entry<Key,Value>>> recordReader, int iteratorCount) {
    this.recordReader = recordReader;
    this.iteratorCount = iteratorCount;
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public AccumuloHiveRow createValue() {
    return new AccumuloHiveRow();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return recordReader.getProgress();
  }

  @Override
  public boolean next(Text rowKey, AccumuloHiveRow row) throws IOException {
    Text key = recordReader.createKey();
    PeekingIterator<Map.Entry<Key,Value>> iter = recordReader.createValue();
    if (recordReader.next(key, iter)) {
      row.clear();
      row.setRowId(key.toString());
      List<Key> keys = Lists.newArrayList();
      List<Value> values = Lists.newArrayList();
      while (iter.hasNext()) { // collect key/values for this row.
        Map.Entry<Key,Value> kv = iter.next();
        keys.add(kv.getKey());
        values.add(kv.getValue());

      }
      if (iteratorCount == 0) { // no encoded values, we can push directly to row.
        pushToValue(keys, values, row);
      } else {
        for (int i = 0; i < iteratorCount; i++) { // each iterator creates a level of encoding.
          SortedMap<Key,Value> decoded = PrimitiveComparisonFilter.decodeRow(keys.get(0),
              values.get(0));
          keys = Lists.newArrayList(decoded.keySet());
          values = Lists.newArrayList(decoded.values());
        }
        pushToValue(keys, values, row); // after decoding we can push to value.
      }

      return true;
    } else {
      return false;
    }
  }

  // flatten key/value pairs into row object for use in Serde.
  private void pushToValue(List<Key> keys, List<Value> values, AccumuloHiveRow row)
      throws IOException {
    Iterator<Key> kIter = keys.iterator();
    Iterator<Value> vIter = values.iterator();
    while (kIter.hasNext()) {
      Key k = kIter.next();
      Value v = vIter.next();
      row.add(k.getColumnFamily().toString(), k.getColumnQualifier().toString(), v.get());
    }
  }
}
