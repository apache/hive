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
import org.apache.hadoop.hive.ql.exec.tez.ReduceRecordProcessor;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

/**
 * A KeyValuesReader implementation that returns a sorted stream of key-values
 * by doing a sorted merge of the key-value in ShuffledMergedInputs.
 * Tags are in the last byte of the key, so no special handling for tags is required.
 * Uses a priority queue to pick the KeyValuesReader of the input that is next in
 * sort order.
 */
public class InputMerger implements KeyValuesReader {

  public static final Log l4j = LogFactory.getLog(ReduceRecordProcessor.class);
  private PriorityQueue<KeyValuesReader> pQueue = null;
  private KeyValuesReader nextKVReader = null;

  public InputMerger(List<ShuffledMergedInput> shuffleInputs) throws IOException {
    //get KeyValuesReaders from the ShuffledMergedInput and add them to priority queue
    int initialCapacity = shuffleInputs.size();
    pQueue = new PriorityQueue<KeyValuesReader>(initialCapacity, new KVReaderComparator());
    for(ShuffledMergedInput input : shuffleInputs){
      addToQueue(input.getReader());
    }
  }

  /**
   * Add KeyValuesReader to queue if it has more key-values
   * @param kvsReadr
   * @throws IOException
   */
  private void addToQueue(KeyValuesReader kvsReadr) throws IOException{
    if(kvsReadr.next()){
      pQueue.add(kvsReadr);
    }
  }

  /**
   * @return true if there are more key-values and advances to next key-values
   * @throws IOException
   */
  public boolean next() throws IOException {
    //add the previous nextKVReader back to queue
    if(nextKVReader != null){
      addToQueue(nextKVReader);
    }

    //get the new nextKVReader with lowest key
    nextKVReader = pQueue.poll();
    return nextKVReader != null;
  }

  public Object getCurrentKey() throws IOException {
    return nextKVReader.getCurrentKey();
  }

  public Iterable<Object> getCurrentValues() throws IOException {
    return nextKVReader.getCurrentValues();
  }

  /**
   * Comparator that compares KeyValuesReader on their current key
   */
  class KVReaderComparator implements Comparator<KeyValuesReader> {

    @Override
    public int compare(KeyValuesReader kvReadr1, KeyValuesReader kvReadr2) {
      try {
        BinaryComparable key1 = (BinaryComparable) kvReadr1.getCurrentKey();
        BinaryComparable key2 = (BinaryComparable) kvReadr2.getCurrentKey();
        return key1.compareTo(key2);
      } catch (IOException e) {
        l4j.error("Caught exception while reading shuffle input", e);
        //die!
        throw new RuntimeException(e);
      }
    }
  }


}
