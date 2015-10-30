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
package org.apache.hadoop.hive.ql.exec.tez.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * A KeyValuesReader implementation that returns a sorted stream of key-values
 * by doing a sorted merge of the key-value in LogicalInputs.
 * Tags are in the last byte of the key, so no special handling for tags is required.
 * Uses a priority queue to pick the KeyValuesReader of the input that is next in
 * sort order.
 */
@SuppressWarnings("deprecation")
public class KeyValueInputMerger extends KeyValueReader {

  public static final Logger l4j = LoggerFactory.getLogger(KeyValueInputMerger.class);
  private PriorityQueue<KeyValueReader> pQueue = null;
  private KeyValueReader nextKVReader = null;
  private ObjectInspector[] inputObjInspectors = null;
  private Deserializer deserializer = null;
  private List<StructField> structFields = null;
  private List<ObjectInspector> fieldOIs = null;
  private final Map<KeyValueReader, List<Object>> kvReaderStandardObjMap =
      new HashMap<KeyValueReader, List<Object>>();

  public KeyValueInputMerger(List<KeyValueReader> multiMRInputs, Deserializer deserializer,
      ObjectInspector[] inputObjInspectors, List<String> sortCols) throws Exception {
    //get KeyValuesReaders from the LogicalInput and add them to priority queue
    int initialCapacity = multiMRInputs.size();
    pQueue = new PriorityQueue<KeyValueReader>(initialCapacity, new KVReaderComparator());
    this.inputObjInspectors = inputObjInspectors;
    this.deserializer = deserializer;
    fieldOIs = new ArrayList<ObjectInspector>();
    structFields = new ArrayList<StructField>();
    StructObjectInspector structOI = (StructObjectInspector) inputObjInspectors[0];
    for (String field : sortCols) {
      StructField sf = structOI.getStructFieldRef(field);
      structFields.add(sf);
      ObjectInspector stdOI =
          ObjectInspectorUtils.getStandardObjectInspector(sf.getFieldObjectInspector());
      fieldOIs.add(stdOI);
    }
    l4j.info("Initialized the priority queue with multi mr inputs: " + multiMRInputs.size());
    for (KeyValueReader input : multiMRInputs) {
      addToQueue(input);
    }
  }

  /**
   * Add KeyValueReader to queue if it has more key-value
   *
   * @param kvReader
   * @throws IOException
   */
  private void addToQueue(KeyValueReader kvReader) throws IOException {
    if (kvReader.next()) {
      kvReaderStandardObjMap.remove(kvReader);
      pQueue.add(kvReader);
    }
  }

  /**
   * @return true if there are more key-values and advances to next key-values
   * @throws IOException
   */
  @Override
  public boolean next() throws IOException {
    //add the previous nextKVReader back to queue
    if(nextKVReader != null){
      addToQueue(nextKVReader);
    }

    //get the new nextKVReader with lowest key
    nextKVReader = pQueue.poll();
    return nextKVReader != null;
  }

  @Override
  public Object getCurrentKey() throws IOException {
    return nextKVReader.getCurrentKey();
  }

  @Override
  public Object getCurrentValue() throws IOException {
    return nextKVReader.getCurrentValue();
  }

  /**
   * Comparator that compares KeyValuesReader on their current key
   */
  class KVReaderComparator implements Comparator<KeyValueReader> {

    @SuppressWarnings({ "unchecked" })
    @Override
    public int compare(KeyValueReader kvReadr1, KeyValueReader kvReadr2) {
      try {
        ObjectInspector oi = inputObjInspectors[0];
        List<Object> row1, row2;
        try {
          if (kvReaderStandardObjMap.containsKey(kvReadr1)) {
            row1 = kvReaderStandardObjMap.get(kvReadr1);
          } else {
            // we need to copy to standard object otherwise deserializer overwrites the values
            row1 =
                (List<Object>) ObjectInspectorUtils.copyToStandardObject(
                    deserializer.deserialize((Writable) kvReadr1.getCurrentValue()), oi,
                    ObjectInspectorCopyOption.WRITABLE);
            kvReaderStandardObjMap.put(kvReadr1, row1);
          }

          if (kvReaderStandardObjMap.containsKey(kvReadr2)) {
            row2 = kvReaderStandardObjMap.get(kvReadr2);
          } else {
            row2 =
                (List<Object>) ObjectInspectorUtils.copyToStandardObject(
                    deserializer.deserialize((Writable) kvReadr2.getCurrentValue()), oi,
                    ObjectInspectorCopyOption.WRITABLE);
            kvReaderStandardObjMap.put(kvReadr2, row2);
          }
        } catch (SerDeException e) {
          throw new IOException(e);
        }

        StructObjectInspector structOI = (StructObjectInspector) oi;
        int compare = 0;
        int index = 0;
        for (StructField sf : structFields) {
          int pos = structOI.getAllStructFieldRefs().indexOf(sf);
          Object key1 = row1.get(pos);
          Object key2 = row2.get(pos);
          ObjectInspector stdOI = fieldOIs.get(index);
          compare = ObjectInspectorUtils.compare(key1, stdOI, key2, stdOI);
          index++;
          if (compare != 0) {
            return compare;
          }
        }

        return compare;
      } catch (IOException e) {
        l4j.error("Caught exception while reading shuffle input", e);
        //die!
        throw new RuntimeException(e);
      }
    }
  }
}
