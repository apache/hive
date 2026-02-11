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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.test.concurrent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.iceberg.hive.HMSTablePropertyHelper;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Annotation to indicate that a test method or test class should run with a mocked
 * HMSTablePropertyHelper.setStorageHandler. This prevents the storage handler from being
 * overwritten during table creation, allowing custom storage handlers to be used in tests.
 *
 * <p>When applied at the class level, all test methods in the class (including their @Before
 * setup methods) will run with the mock active.
 *
 * <p>When applied at the method level, only that specific test method (including @Before for
 * that test) will run with the mock active.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface WithMockedStorageHandler {

  /**
   * JUnit Rule that mocks HMSTablePropertyHelper.setStorageHandler for tests annotated with
   * {@link WithMockedStorageHandler}. This allows tests to use custom storage handlers without
   * having them overwritten by the default Iceberg storage handler.
   */
  class Rule implements TestRule {

    private static final Method setStorageHandlerMethod;

    static {
      try {
        setStorageHandlerMethod = HMSTablePropertyHelper.class
            .getDeclaredMethod("setStorageHandler", Map.class, Boolean.TYPE);
        setStorageHandlerMethod.setAccessible(true);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Failed to initialize WithMockedStorageHandler.Rule", e);
      }
    }

    @Override
    public Statement apply(Statement base, Description description) {
      // Check for annotation at method level OR class level
      WithMockedStorageHandler annotation = description.getAnnotation(WithMockedStorageHandler.class);
      if (annotation == null) {
        annotation = description.getTestClass().getAnnotation(WithMockedStorageHandler.class);
      }

      if (annotation == null) {
        // No annotation, run test normally
        return base;
      }

      // Annotation present, wrap with mock (covers @Before + @Test)
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          try (MockedStatic<HMSTablePropertyHelper> tableOps =
                   mockStatic(HMSTablePropertyHelper.class, CALLS_REAL_METHODS)) {
            tableOps.when(() -> setStorageHandlerMethod.invoke(null, anyMap(), eq(true)))
                .thenAnswer(invocation -> null);
            base.evaluate();
          }
        }
      };
    }
  }
}
