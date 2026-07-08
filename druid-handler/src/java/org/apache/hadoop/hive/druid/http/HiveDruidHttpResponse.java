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

package org.apache.hadoop.hive.druid.http;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * HTTP response wrapper for Druid REST calls. Replaces Druid's Netty 3 based response holders (HIVE-25013).
 */
public class HiveDruidHttpResponse {
  public static final int SC_OK = 200;
  public static final int SC_BAD_REQUEST = 400;
  public static final int SC_UNAUTHORIZED = 401;
  public static final int SC_NOT_FOUND = 404;
  public static final int SC_TEMPORARY_REDIRECT = 307;

  private final int statusCode;
  private final byte[] body;
  private final Map<String, List<String>> headers;

  public HiveDruidHttpResponse(int statusCode, byte[] body, Map<String, List<String>> headers) {
    this.statusCode = statusCode;
    this.body = body == null ? new byte[0] : body;
    this.headers = headers == null ? Collections.emptyMap() : headers;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public byte[] getBody() {
    return body;
  }

  public String getContent() {
    return getContent(StandardCharsets.UTF_8);
  }

  public String getContent(Charset charset) {
    return new String(body, charset);
  }

  public String getHeader(String name) {
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(name) && !entry.getValue().isEmpty()) {
        return entry.getValue().get(0);
      }
    }
    return null;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }
}
