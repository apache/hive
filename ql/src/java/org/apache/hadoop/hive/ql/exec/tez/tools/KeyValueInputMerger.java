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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * A KeyValuesReader implementation that returns a sorted stream of key-values
 * by doing a sorted merge of the key-value in LogicalInputs.
 * Tags are in the last byte of the key, so no special handling for tags is required.
 * Uses a priority queue to pick the KeyValuesReader of the input that is next in
 * sort order.
 */
public class KeyValueInputMerger extends KeyValueReader {

  public static final Log l4j = LogFactory.getLog(KeyValueInputMerger.class);
  private PriorityQueue<KeyValueReader> pQueue = null;
  private KeyValueReader nextKVReader = null;

  public KeyValueInputMerger(List<KeyValueReader> multiMRInputs) throws Exception {
    //get KeyValuesReaders from the LogicalInput and add them to priority queue
    int initialCapacity = multiMRInputs.size();
    pQueue = new PriorityQueue<KeyValueReader>(initialCapacity, new KVReaderComparator());
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

    @Override
    public int compare(KeyValueReader kvReadr1, KeyValueReader kvReadr2) {
      try {
        BinaryComparable key1 = (BinaryComparable) kvReadr1.getCurrentValue();
        BinaryComparable key2 = (BinaryComparable) kvReadr2.getCurrentValue();
        return key1.compareTo(key2);
      } catch (IOException e) {
        l4j.error("Caught exception while reading shuffle input", e);
        //die!
        throw new RuntimeException(e);
      }
    }
  }
}
