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

package org.apache.hive.hep;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Loads {@code .erp} policy fixtures from the test classpath.
 * Each fixture lives under {@code src/test/resources/policies/} and is read
 * verbatim as a UTF-8 string.
 */
final class PolicyTestResources {

  private static final String BASE = "/policies/";

  private PolicyTestResources() {
  }

  /**
   * Load the named {@code .erp} fixture (e.g. {@code "example.erp"}).
   *
   * @throws IllegalStateException if the resource cannot be found
   * @throws RuntimeException if reading fails
   */
  static String load(String name) {
    String path = BASE + name;
    try (InputStream in = PolicyTestResources.class.getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("missing test resource: " + path);
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = in.read(buf)) != -1) {
        baos.write(buf, 0, n);
      }
      return baos.toString(StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      throw new RuntimeException("failed to read resource " + path, e);
    }
  }
}
