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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit test rule that reruns test on failure. With Retry test rule only the test method will be retried,
 * the test class will not be re-initialized.
 */
public class Retry implements TestRule {

  private final int retryCount;

  public Retry() {
    this(RetryTestRunner.DEFAULT_RETRY_COUNT);
  }

  public Retry(final int retryCount) {
    this.retryCount = retryCount;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new RetryingStatement(base, description);
  }

  private class RetryingStatement extends Statement {
    private final Statement wrappedStatement;
    private final Description description;

    private RetryingStatement(Statement wrappedStatement, final Description description) {
      this.wrappedStatement = wrappedStatement;
      this.description = description;
    }

    @Override
    public void evaluate() throws Throwable {
      int failedAttempts = 0;
      boolean retry;
      do {
        try {
          wrappedStatement.evaluate();
          retry = false;
        } catch (Throwable throwable) {
          if (retryCount > failedAttempts) {
            failedAttempts++;
            retry = true;
            System.out.println(description + " Caught: " + throwable.getMessage() + ". Retrying test " +
              failedAttempts + "/" + retryCount);
          } else {
            throw throwable;
          }
        }
      } while (retry);
    }
  }
}