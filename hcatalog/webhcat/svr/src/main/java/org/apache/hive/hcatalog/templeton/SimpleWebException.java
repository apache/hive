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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Simple exception that will return a json error payload if thrown
 * from a JAX web server.  We skip using WebApplicationException and
 * instead map our own so that Jersey doesn't log our exceptions as
 * error in the output log.  See SimpleExceptionMapper.
 */
public class SimpleWebException extends Throwable {
  public int httpCode;
  public Map<String, Object> params;

  public SimpleWebException(int httpCode, String msg) {
    super(msg);
    this.httpCode = httpCode;
  }

  public SimpleWebException(int httpCode, String msg, Map<String, Object> params) {
    super(msg);
    this.httpCode = httpCode;
    this.params = params;
  }

  public Response getResponse() {
    return buildMessage(httpCode, params, getMessage());
  }

  public static Response buildMessage(int httpCode, Map<String, Object> params,
                    String msg) {
    HashMap<String, Object> err = new HashMap<String, Object>();
    err.put("error", msg);
    if (params != null)
      err.putAll(params);

    String json = "\"error\"";
    try {
      json = new ObjectMapper().writeValueAsString(err);
    } catch (IOException e) {
    }

    return Response.status(httpCode)
      .entity(json)
      .type(MediaType.APPLICATION_JSON)
      .build();
  }
}
