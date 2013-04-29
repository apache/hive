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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Base class for VectorizedRowBatch data source.
 * Used in unit test only.
 *
 */
public abstract class FakeVectorRowBatchBase implements Iterable<VectorizedRowBatch> {
  public abstract VectorizedRowBatch produceNextBatch();

  private boolean iteratorProduced;
  public Iterator<VectorizedRowBatch> iterator() {
    assert (!iteratorProduced);
    return this.new BatchIterator();
  }

  private class BatchIterator implements Iterator<VectorizedRowBatch> {
    private VectorizedRowBatch currentBatch;

    private VectorizedRowBatch getCurrentBatch() {
      if (null == currentBatch) {
        currentBatch = produceNextBatch();
      }
      return currentBatch;
    }

    public boolean hasNext() {
      return getCurrentBatch().size > 0;
    }

    public VectorizedRowBatch next() {
      VectorizedRowBatch ret = getCurrentBatch();
      currentBatch = null;
      return ret;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

