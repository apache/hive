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
package org.apache.hive.common.util;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryUtilities {
  public static class RetryException extends Exception {
    private static final long serialVersionUID = 1L;

    public RetryException(Exception ex) {
      super(ex);
    }

    public RetryException(String msg) {
      super(msg);
    }
  }

  /**
   * Interface used to create a ExponentialBackOffRetry policy
   */
  public static interface ExponentialBackOffRetry<T> {
    /**
     * This method should be called by implementations of this ExponentialBackOffRetry policy
     * It represents the actual work which needs to be done based on a given batch size
     * @param batchSize The batch size for the work which needs to be executed
     * @return
     * @throws Exception
     */
    public T execute(int batchSize) throws Exception;
  }

  /**
   * This class is a base implementation of a simple exponential back retry policy. The batch size
   * and decaying factor are provided with the constructor. It reduces the batch size by dividing
   * it by the decaying factor every time there is an exception in the execute method.
   */
  public static abstract class ExponentiallyDecayingBatchWork<T>
      implements ExponentialBackOffRetry<T> {
    private int batchSize;
    private final int decayingFactor;
    private int maxRetries;
    private static final Logger LOG = LoggerFactory.getLogger(ExponentiallyDecayingBatchWork.class);

    public ExponentiallyDecayingBatchWork(int batchSize, int reducingFactor, int maxRetries) {
      if (batchSize <= 0) {
        throw new IllegalArgumentException(String.format(
            "Invalid batch size %d provided. Batch size must be greater than 0", batchSize));
      }
      this.batchSize = batchSize;
      if (reducingFactor <= 1) {
        throw new IllegalArgumentException(String.format(
            "Invalid decaying factor %d provided. Decaying factor must be greater than 1",
            batchSize));
      }
      if (maxRetries < 0) {
        throw new IllegalArgumentException(String.format(
            "Invalid number of maximum retries %d provided. It must be a non-negative integer value",
            maxRetries));
      }
      //if maxRetries is 0 code retries until batch decays to zero
      this.maxRetries = maxRetries;
      this.decayingFactor = reducingFactor;
    }

    public T run() throws Exception {
      int attempt = 0;
      while (true) {
        int size = getNextBatchSize();
        if (size == 0) {
          throw new RetryException("Batch size reduced to zero");
        }
        try {
          return execute(size);
        } catch (Exception ex) {
          LOG.warn(String.format("Exception thrown while processing using a batch size %d", size),
              ex);
        } finally {
          attempt++;
          if (attempt == maxRetries) {
            throw new RetryException(String.format("Maximum number of retry attempts %d exhausted", maxRetries));
          }
        }
      }
    }

    private int getNextBatchSize() {
      int ret = batchSize;
      batchSize /= decayingFactor;
      return ret;
    }
  }
}
