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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Provides a key/values (note the plural values) interface out of a KeyValueReader,
 * needed by ReduceRecordSource when reading input from a key/value source.
 */
public class KeyValuesFromKeyValue implements KeyValuesAdapter {
  protected KeyValueReader reader;
  protected ValueIterator<Object> valueIterator =
      new ValueIterator<Object>();

  private static class ValueIterator<T> implements Iterator<T>, Iterable<T> {

    protected boolean hasNextValue = false;
    protected T value = null;

    @Override
    public boolean hasNext() {
      return hasNextValue;
    }

    @Override
    public T next() {
      if (!hasNextValue) {
        throw new NoSuchElementException();
      }
      hasNextValue = false;
      return value;
    }

    void reset(T value) {
      this.value = value;
      hasNextValue = true;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }
  }

  public KeyValuesFromKeyValue(KeyValueReader reader) {
    this.reader = reader;
  }

  @Override
  public Object getCurrentKey() throws IOException {
    return reader.getCurrentKey();
  }

  @Override
  public Iterable<Object> getCurrentValues() throws IOException {
    Object obj = reader.getCurrentValue();
    valueIterator.reset(obj);
    return valueIterator;
  }

  @Override
  public boolean next() throws IOException {
    return reader.next();
  }
}