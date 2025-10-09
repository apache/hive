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
package org.apache.hive.jdbc;

import java.io.IOException;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;

public class HttpDefaultResponseInterceptor extends HttpResponseInterceptorBase {

  @Override
  public void process(HttpResponse response, HttpContext context) throws HttpException, IOException {
    String trackHeader = (String) context.getAttribute(Constants.HTTP_HEADER_REQUEST_TRACK);
    if (trackHeader == null) {
      return;
    }
    String trackTimeHeader = trackHeader + Constants.TIME_POSTFIX_REQUEST_TRACK;
    long elapsed = Time.monotonicNow() - (long) context.getAttribute(trackTimeHeader);
    LOG.info("Response to {} in {} ms", trackHeader, elapsed);
    context.removeAttribute(Constants.HTTP_HEADER_REQUEST_TRACK);
    context.removeAttribute(trackTimeHeader);
  }
}
