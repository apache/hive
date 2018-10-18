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

import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * JUnit test runner that reruns test on failure.
 */
public class RetryTestRunner extends BlockJUnit4ClassRunner {
  // TODO: should this be configurable via annotation or extending @RunWith annotation?
  static final int DEFAULT_RETRY_COUNT = 2; // test is executed 3 times in worst case 1 original + 2 retries
  private final int retryCount;
  private int failedAttempts = 0;

  public RetryTestRunner(final Class<?> klass) throws InitializationError {
    super(klass);
    this.retryCount = DEFAULT_RETRY_COUNT;
  }

  // from ParentRunner, retried under exception (notified only after exhausting retryCount)
  // invoked for test classes
  @Override
  public void run(final RunNotifier notifier) {
    final Description description = getDescription();
    final EachTestNotifier testNotifier = new EachTestNotifier(notifier, description);
    final Statement statement = classBlock(notifier);
    try {
      statement.evaluate();
    } catch (AssumptionViolatedException e) {
      testNotifier.fireTestIgnored();
    } catch (StoppedByUserException e) {
      // not retrying when user explicitly stops the test
      throw e;
    } catch (Throwable e) {
      // retry on any other exception
      retry(description, testNotifier, statement, e);
    }
  }

  // invoked for test methods
  @Override
  protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
    final Description description = describeChild(method);
    if (method.getAnnotation(Ignore.class) != null) {
      notifier.fireTestIgnored(description);
    } else {
      runTestUnit(methodBlock(method), description, notifier);
    }
  }

  private void runTestUnit(final Statement statement, final Description description, final RunNotifier notifier) {
    final EachTestNotifier eachNotifier = new EachTestNotifier(notifier, description);
    eachNotifier.fireTestStarted();
    try {
      statement.evaluate();
    } catch (AssumptionViolatedException e) {
      eachNotifier.addFailedAssumption(e);
    } catch (Throwable e) {
      retry(description, eachNotifier, statement, e);
    } finally {
      eachNotifier.fireTestFinished();
    }
  }

  private void retry(final Description description, final EachTestNotifier notifier,
    final Statement statement, final Throwable currentThrowable) {
    Throwable caughtThrowable = currentThrowable;
    while (retryCount > failedAttempts) {
      try {
        System.out.println(description + " Caught: " + (currentThrowable == null ? "exception" :
          currentThrowable.getMessage()) + ". Retrying test " + failedAttempts + "/" + retryCount);
        statement.evaluate();
        return;
      } catch (Throwable t) {
        failedAttempts++;
        caughtThrowable = t;
      }
    }
    notifier.addFailure(caughtThrowable);
  }
}