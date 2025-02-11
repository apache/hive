/* * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hive.metastore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Secures servlet processing.
 */
public interface SecureServletCaller {
  /**
   * Should be called in Servlet.init()
   * @throws ServletException if the jwt validator creation throws an exception
   */
  void init() throws ServletException;

  /**
   * Any http method executor.
   */
  @FunctionalInterface
  interface MethodExecutor {
    /**
     * The method to call to secure the execution of a (http) method.
     * @param request the request
     * @param response the response
     * @throws ServletException if the method executor fails
     * @throws IOException if the Json in/out fail
     */
    void execute(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException;
  }

  /**
   * The method to call to secure the execution of a (http) method.
   * @param request the request
   * @param response the response
   * @param executor the method executor
   */
   void execute(HttpServletRequest request, HttpServletResponse response, MethodExecutor executor)
      throws IOException;


}
