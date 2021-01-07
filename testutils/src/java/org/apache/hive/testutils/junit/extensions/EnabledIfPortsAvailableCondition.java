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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

/**
 * JUnit Jupiter extension to selectively enable tests when certain ports are available.
 *
 * @see EnabledIfPortsAvailable
 */
class EnabledIfPortsAvailableCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    return findAnnotation(context.getElement(), EnabledIfPortsAvailable.class) //
        .map(annotation -> {
          for (int port : annotation.value()) {
            if (!isPortAvailable(port)) {
              return ConditionEvaluationResult.disabled("Port " + port + " is not available.");
            }
          }
          return ConditionEvaluationResult.enabled("All required ports are free");
        }).orElse(ConditionEvaluationResult.enabled(""));
  }

  private static boolean isPortAvailable(int port) {
    try (@SuppressWarnings("unused") ServerSocket ss = new ServerSocket(port);
        @SuppressWarnings("unused") DatagramSocket ds = new DatagramSocket(port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
