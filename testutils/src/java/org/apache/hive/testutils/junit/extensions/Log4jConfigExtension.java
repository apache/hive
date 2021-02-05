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
package org.apache.hive.testutils.junit.extensions;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.AnnotatedElement;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A JUnit Jupiter extension that loads a log4j2 configuration from a file present in the resources of the project
 * when the test uses the {@link Log4jConfig} annotation.
 *
 * The configuration that was in use before starting the annotated test is restored when the test completes
 * (success, or failure).
 *
 * The class is thread-safe but tests with {@link Log4jConfig} cannot run in parallel due to the nature of the log4j
 * configuration.
 *
 * @see Log4jConfig
 */
public class Log4jConfigExtension implements BeforeEachCallback, AfterEachCallback {
  private static final ReentrantLock LOCK = new ReentrantLock();
  private static Configuration oldConfig = null;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    if (!LOCK.tryLock(1, TimeUnit.MINUTES)) {
      throw new IllegalStateException(
          "Lock acquisition failed cause another test is using a custom Log4j configuration.");
    }
    assert oldConfig == null;
    LoggerContext ctx = LoggerContext.getContext(false);
    oldConfig = ctx.getConfiguration();
    Optional<AnnotatedElement> element = context.getElement();
    if (!element.isPresent()) {
      throw new IllegalStateException("The extension needs an annotated method");
    }
    Log4jConfig annotation = element.get().getAnnotation(Log4jConfig.class);
    if (annotation == null) {
      throw new IllegalStateException(Log4jConfig.class.getSimpleName() + " annotation is missing.");
    }
    String configFileName = annotation.value();
    URL config = Log4jConfig.class.getClassLoader().getResource(configFileName);
    if (config == null) {
      throw new IllegalStateException("File " + configFileName + " was not found in resources.");
    }
    ctx.setConfigLocation(config.toURI());
  }

  @Override
  public void afterEach(ExtensionContext context) {
    try {
      LoggerContext ctx = LoggerContext.getContext(false);
      ctx.setConfiguration(oldConfig);
      oldConfig = null;
    } finally {
      LOCK.unlock();
    }
  }
}
