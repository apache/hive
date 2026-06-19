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

package org.apache.hadoop.hive.ql.exec.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Tests for retriable interface.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRetryable {

  @Mock
  UserGroupInformation userGroupInformation;

  MockedStatic<UserGroupInformation> userGroupInformationMockedStatic;

  @Before
  public void setup() throws IOException {
    userGroupInformationMockedStatic = Mockito.mockStatic(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
    Mockito.when(UserGroupInformation.getLoginUser()).thenReturn(userGroupInformation);
    Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
  }

  @After
  public void tearDown() {
    userGroupInformationMockedStatic.close();
  }

  @Test
  public void testRetrySuccessValidException() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(10)
      .withInitialDelay(1)
      .withBackoff(1.0)
      .withRetryOnException(NullPointerException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count < 1) {
            count++;
            throw new NullPointerException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testRetrySuccessValidExceptionList() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(60)
      .withInitialDelay(1)
      .withBackoff(1.0)
      .withRetryOnExceptionList(Arrays.asList(NullPointerException.class, IOException.class)).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count == 0) {
            count++;
            throw new NullPointerException();
          } else if (count == 1) {
            count++;
            throw new IOException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testRetryFailureWithMaxDuration() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(10)
      .withBackoff(10.0)
      .withInitialDelay(1)
      .withRetryOnException(NullPointerException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count < 2) {
            count++;
            throw new NullPointerException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(NullPointerException.class, e.getClass());
    }
  }

  @Test
  public void testRetryFailureWithInitialDelay() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(20)
      .withBackoff(10.0)
      .withInitialDelay(10)
      .withMaxJitterValue(1)
      .withRetryOnExceptionList(Arrays.asList(NullPointerException.class, IOException.class)).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count == 0) {
            count++;
            throw new NullPointerException();
          } else if (count == 1) {
            count++;
            throw new IOException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(IOException.class, e.getClass());
    }
  }

  @Test
  public void testRetryFailureWithMaxRetryDelay() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(20)
      .withBackoff(10.0)
      .withInitialDelay(1)
      .withMaxJitterValue(1)
      .withMaxRetryDelay(1)
      .withRetryOnExceptionList(Arrays.asList(NullPointerException.class, IOException.class)).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count == 0) {
            count++;
            throw new NullPointerException();
          } else if (count == 1) {
            count++;
            throw new IOException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testRetryFailureWithBackoff() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(20)
      .withBackoff(100.0)
      .withInitialDelay(1)
      .withMaxJitterValue(1)
      .withRetryOnException(NullPointerException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count < 2) {
            count++;
            throw new NullPointerException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(NullPointerException.class, e.getClass());
    }
  }

  @Test
  public void testRetrySuccessWithMaxDurationDifferentException() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(30)
      .withBackoff(10.0)
      .withInitialDelay(1)
      .withMaxJitterValue(1)
      .withRetryOnExceptionList(Arrays.asList(NullPointerException.class, IOException.class)).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count == 0) {
            count++;
            throw new NullPointerException();
          } else if (count == 1) {
            count++;
            throw new IOException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testRetrySuccessWithNonRetriableException() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(30)
      .withBackoff(10.0)
      .withInitialDelay(1)
      .withMaxJitterValue(1)
      .withRetryOnException(IOException.class)
      .withFailOnException(FileNotFoundException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        int count = 0;
        @Override
        public Void call() throws Exception {
          if (count == 0) {
            count++;
            throw new FileNotFoundException();
          } else if (count == 1) {
            count++;
            throw new IOException();
          } else {
            return null;
          }
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(FileNotFoundException.class, e.getClass());
    }
  }

  @Test
  public void testRetryFailureInValidException() throws Throwable {
    Retryable retryable = Retryable.builder()
      .withTotalDuration(10)
      .withInitialDelay(1)
      .withBackoff(1.0)
      .withRetryOnException(NullPointerException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          throw new IOException();
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(IOException.class, e.getClass());
    }
  }

  @Test
  public void testRetryFailureWithHiveConf() throws Throwable {
    HiveConf conf = new HiveConf(TestRetryable.class);
    conf.set(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.varname, "1s");
    conf.setFloat(HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT.varname, 1.0f);
    conf.set(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION.varname, "60s");
    conf.set(HiveConf.ConfVars.REPL_RETRY_JITTER.varname, "1s");
    conf.set(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES.varname, "30s");
    long startTime = System.currentTimeMillis();
    long totalTime = 60;
    Retryable retryable = Retryable.builder().withHiveConf(conf)
      .withRetryOnException(NullPointerException.class).build();
    try {
      retryable.executeCallable(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          executeWithDelay(startTime, totalTime);
          return null;
        }
      });
    } catch (Exception e) {
      Assert.assertEquals(NullPointerException.class, e.getClass());
    }
  }

  @Test
  public void testRetrySuccessSecureCallable() throws Throwable {
    Mockito.when(userGroupInformation.doAs((PrivilegedExceptionAction<Boolean>) Mockito.any())).thenCallRealMethod();
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
    Retryable retryable = Retryable.builder()
      .withTotalDuration(10)
      .withInitialDelay(1)
      .withBackoff(1.0)
      .withRetryOnExceptionList(Arrays.asList(InterruptedException.class, IOException.class)).build();
    try {
      retryable.executeCallable(() -> true);
    } catch (Exception e) {
      Assert.fail();
    }
    ArgumentCaptor<PrivilegedExceptionAction> privilegedActionArgumentCaptor
      = ArgumentCaptor.forClass(PrivilegedExceptionAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(1)).doAs(privilegedActionArgumentCaptor.capture());
  }

  private void executeWithDelay(long startTime, long totalTime) {
    long currentTime = System.currentTimeMillis();
    if (currentTime - startTime < totalTime * 1000) {
      throw new NullPointerException();
    }
  }
}
