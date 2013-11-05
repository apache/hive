/**
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
package org.apache.hadoop.hive.shims;

import java.io.IOException;

/**
 * Since Hadoop ships with different versions of Jetty in different versions,
 * Hive uses a shim layer to access the parts of the API that have changed.
 * Users should obtain an instance of this class using the ShimLoader factory.
 */
public interface JettyShims {

  Server startServer(String listen, int port) throws IOException;

  /**
   * Server.
   *
   */
  interface Server {
    void addWar(String war, String mount);

    void start() throws Exception;

    void join() throws InterruptedException;

    void stop() throws Exception;
  }
}