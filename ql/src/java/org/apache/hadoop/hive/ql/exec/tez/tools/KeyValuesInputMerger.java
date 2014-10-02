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
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * A KeyValuesReader implementation that returns a sorted stream of key-values
 * by doing a sorted merge of the key-value in LogicalInputs.
 * Tags are in the last byte of the key, so no special handling for tags is required.
 * Uses a priority queue to pick the KeyValuesReader of the input that is next in
 * sort order.
 */
public class KeyValuesInputMerger extends KeyValuesReader {

  private class KeyValuesIterable implements Iterable<Object> {

    KeyValuesIterator currentIterator = null;

    KeyValuesIterable(int size) {
      currentIterator = new KeyValuesIterator(size);
    }

    @Override
    public Iterator<Object> iterator() {
      return currentIterator;
    }

    public void init(List<KeyValuesReader> readerList) {
      currentIterator.init(readerList);
    }
  }

  private class KeyValuesIterator implements Iterator<Object> {
    KeyValuesReader[] readerArray = null;
    Iterator<Object> currentIterator = null;
    int currentIndex = 0;
    int loadedSize = 0;

    KeyValuesIterator(int size) {
      readerArray = new KeyValuesReader[size];
    }

    public void init(List<KeyValuesReader> readerList) {
      for (int i = 0; i < readerList.size(); i++) {
        readerArray[i] = null;
      }
      loadedSize = 0;
      for (KeyValuesReader kvsReader : readerList) {
        readerArray[loadedSize] = kvsReader;
        loadedSize++;
      }
      currentIterator = null;
      currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
      if ((currentIterator == null) || (currentIterator.hasNext() == false)) {
        if (currentIndex == loadedSize) {
          return false;
        }

        try {
          if (readerArray[currentIndex] == null) {
            return false;
          }
          currentIterator = readerArray[currentIndex].getCurrentValues().iterator();
          currentIndex++;
          return currentIterator.hasNext();
        } catch (IOException e) {
          return false;
        }
      }

      return true;
    }

    @Override
    public Object next() {
      l4j.info("next called on " + currentIterator);
      return currentIterator.next();
    }

    @Override
    public void remove() {
      // nothing to do
    }
  }

  public static final Log l4j = LogFactory.getLog(KeyValuesInputMerger.class);
  private PriorityQueue<KeyValuesReader> pQueue = null;
  private final List<KeyValuesReader> nextKVReaders = new ArrayList<KeyValuesReader>();
  KeyValuesIterable kvsIterable = null;

  public KeyValuesInputMerger(List<? extends Input> shuffleInputs) throws Exception {
    //get KeyValuesReaders from the LogicalInput and add them to priority queue
    int initialCapacity = shuffleInputs.size();
    kvsIterable = new KeyValuesIterable(initialCapacity);
    pQueue = new PriorityQueue<KeyValuesReader>(initialCapacity, new KVReaderComparator());
    for(Input input : shuffleInputs){
      addToQueue((KeyValuesReader)input.getReader());
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
  @Override
  public boolean next() throws IOException {
    //add the previous nextKVReader back to queue
    if (!nextKVReaders.isEmpty()) {
      for (KeyValuesReader kvReader : nextKVReaders) {
        addToQueue(kvReader);
      }
      nextKVReaders.clear();
    }

    KeyValuesReader nextKVReader = null;
    //get the new nextKVReader with lowest key
    nextKVReader = pQueue.poll();
    if (nextKVReader != null) {
      nextKVReaders.add(nextKVReader);
    }

    while (pQueue.peek() != null) {
      KeyValuesReader equalValueKVReader = pQueue.poll();
      if (pQueue.comparator().compare(nextKVReader, equalValueKVReader) == 0) {
        nextKVReaders.add(equalValueKVReader);
      } else {
        pQueue.add(equalValueKVReader);
        break;
      }
    }
    return !(nextKVReaders.isEmpty());
  }

  @Override
  public Object getCurrentKey() throws IOException {
    // return key from any of the readers
    return nextKVReaders.get(0).getCurrentKey();
  }

  @Override
  public Iterable<Object> getCurrentValues() throws IOException {
    kvsIterable.init(nextKVReaders);
    return kvsIterable;
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
