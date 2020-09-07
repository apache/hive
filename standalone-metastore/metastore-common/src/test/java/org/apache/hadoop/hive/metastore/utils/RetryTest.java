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

package org.apache.hadoop.hive.metastore.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for retriable interface.
 */
public class RetryTest {
  @Test
  public void testRetrySuccess() {
    Retry<Void> retriable = new Retry<Void>(NullPointerException.class) {
      private int count = 0;
      @Override
      public Void execute() {
        if (count < 1) {
          count++;
          throw new NullPointerException();
        } else {
          return null;
        }
      }
    };
    try {
      retriable.run();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testRetryFailure() {
    Retry<Void> retriable = new Retry<Void>(NullPointerException.class) {
      @Override
      public Void execute() {
        throw new RuntimeException();
      }
    };
    try {
      retriable.run();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(RuntimeException.class, e.getClass());
    }
  }

  @Test
  public void testRetryFailureWithDelay() {
    Retry<Void> retriable = new Retry<Void>(NullPointerException.class) {
      @Override
      public Void execute() {
        throw new RuntimeException();
      }
    };
    try {
      retriable.runWithDelay();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(RuntimeException.class, e.getClass());
    }
  }

  @Test
  public void testRetrySuccessWithDelay() {
    Retry<Void> retriable = new Retry<Void>(NullPointerException.class) {
      private long startTime = System.currentTimeMillis();
      @Override
      public Void execute() {
        executeWithDelay(startTime);
        return null;
      }
    };
    try {
      retriable.runWithDelay();
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void executeWithDelay(long startTime) {
    long currentTime = System.currentTimeMillis();
    if (currentTime - startTime < 40 * 1000) {
      throw new NullPointerException();
    }
  }

  @Test
  public void testRetryFailureWithDelayMoreThanTimeout() {
    Retry<Void> retriable = new Retry<Void>(NullPointerException.class) {
      @Override
      public Void execute() {
        throw new NullPointerException();
      }
    };
    long startTime = System.currentTimeMillis();
    try {
      retriable.runWithDelay();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(NullPointerException.class, e.getClass());
      Assert.assertTrue(System.currentTimeMillis() - startTime >= 180 * 1000);
    }
  }
}
