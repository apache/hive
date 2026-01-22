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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 *  Base class to add the batch process.
 *  1. Provide the implementation of run() to process one batch
 *  2. Call runBatched() to process the whole dataset
 *
 *  I: input type, R: result type
 */
public abstract class Batchable<I, R> {
  public static final int NO_BATCHING = -1;

  public abstract List<R> run(List<I> input) throws Exception;

  public final List<R> runBatched(
      final int batchSize,
      List<I> input) throws MetaException {
    if (input == null || input.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      if (batchSize == NO_BATCHING || batchSize >= input.size()) {
        return run(input);
      }
      List<R> result = new ArrayList<>(input.size());
      for (int fromIndex = 0, toIndex = 0; toIndex < input.size(); fromIndex = toIndex) {
        toIndex = Math.min(fromIndex + batchSize, input.size());
        List<I> batchedInput = input.subList(fromIndex, toIndex);
        List<R> batchedOutput = run(batchedInput);
        if (batchedOutput != null) {
          result.addAll(batchedOutput);
        }
      }
      return result;
    } catch (Exception e) {
      throw ExceptionHandler.newMetaException(e);
    }
  }
}
