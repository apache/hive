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

package org.apache.hadoop.hive.druid.security;

import com.google.common.collect.Maps;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;

/**
 * Class to handle Cookies used to cache the Kerberos Credentials.
 * @param <Intermediate> intermediate response type.
 * @param <Final> final response type.
 */
public class ResponseCookieHandler<Intermediate, Final> implements HttpResponseHandler<Intermediate, Final> {
  private static final Logger LOG = LoggerFactory.getLogger(ResponseCookieHandler.class);

  private final URI uri;
  private final CookieManager manager;
  private final HttpResponseHandler<Intermediate, Final> delegate;

  ResponseCookieHandler(URI uri, CookieManager manager, HttpResponseHandler<Intermediate, Final> delegate) {
    this.uri = uri;
    this.manager = manager;
    this.delegate = delegate;
  }

  @Override public ClientResponse<Intermediate> handleResponse(HttpResponse httpResponse, TrafficCop trafficCop) {
    try {
      final HttpHeaders headers = httpResponse.headers();
      manager.put(uri, Maps.asMap(headers.names(), headers::getAll));
      return delegate.handleResponse(httpResponse, trafficCop);
    } catch (IOException e) {
      LOG.error("Error while processing Cookies from header", e);
      throw new RuntimeException(e);
    }
  }

  @Override public ClientResponse<Intermediate> handleChunk(ClientResponse<Intermediate> clientResponse,
      HttpChunk httpChunk,  long chunkNum) {
    return delegate.handleChunk(clientResponse, httpChunk, chunkNum);
  }

  @Override public ClientResponse<Final> done(ClientResponse<Intermediate> clientResponse) {
    return delegate.done(clientResponse);
  }

  @Override public void exceptionCaught(ClientResponse<Intermediate> clientResponse, Throwable throwable) {
    delegate.exceptionCaught(clientResponse, throwable);
  }
}
