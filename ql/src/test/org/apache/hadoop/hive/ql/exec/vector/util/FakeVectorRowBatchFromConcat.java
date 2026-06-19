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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Concatenates many arbitrary FakeVectorRowBatch sources as one single source.
 * Used in unit test only.
 *
 */
public class FakeVectorRowBatchFromConcat extends FakeVectorRowBatchBase {

  private Iterable<VectorizedRowBatch>[] iterables;
  private Iterator<VectorizedRowBatch> iterator;
  private int index;
  private static VectorizedRowBatch emptyBatch = new VectorizedRowBatch(0, 0);

  public FakeVectorRowBatchFromConcat(Iterable<VectorizedRowBatch>...iterables) {
    this.iterables = iterables;
  }

  @Override
  public VectorizedRowBatch produceNextBatch() {
    VectorizedRowBatch ret = null;

    do {
      if (null != iterator && iterator.hasNext()) {
        ret = iterator.next();
        break;
      }

      if (index >= iterables.length) {
        ret = emptyBatch;
        break;
      }

      iterator = iterables[index].iterator();
      ++index;
    } while (true);

    return ret;
  }
}

