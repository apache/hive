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

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP request for Druid REST calls. Replaces Druid's Netty 3 based Request (HIVE-25013).
 */
public class HiveDruidHttpRequest {
  private final String method;
  private final URL url;
  private final Map<String, List<String>> headers = new HashMap<>();
  private byte[] content;

  public HiveDruidHttpRequest(String method, URL url) {
    this.method = method;
    this.url = url;
  }

  public String getMethod() {
    return method;
  }

  public URL getUrl() {
    return url;
  }

  public byte[] getContent() {
    return content;
  }

  public boolean hasContent() {
    return content != null && content.length > 0;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }

  public HiveDruidHttpRequest setContent(byte[] content) {
    this.content = content;
    return this;
  }

  public HiveDruidHttpRequest setHeader(String name, String value) {
    List<String> values = new ArrayList<>(1);
    values.add(value);
    headers.put(name, values);
    return this;
  }

  public HiveDruidHttpRequest addHeaderValues(String name, List<String> values) {
    headers.put(name, new ArrayList<>(values));
    return this;
  }

  public HiveDruidHttpRequest copy() {
    HiveDruidHttpRequest copy = new HiveDruidHttpRequest(method, url);
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      copy.headers.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    if (content != null) {
      copy.content = content.clone();
    }
    return copy;
  }

  public HiveDruidHttpRequest withUrl(URL newUrl) {
    HiveDruidHttpRequest copy = new HiveDruidHttpRequest(method, newUrl);
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      copy.headers.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    if (content != null) {
      copy.content = content.clone();
    }
    return copy;
  }

  public HiveDruidHttpRequest setContent(String content) {
    return setContent(content.getBytes(StandardCharsets.UTF_8));
  }
}
