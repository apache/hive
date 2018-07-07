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
package org.apache.hadoop.hive.registry.common.exception;

import org.apache.hadoop.hive.registry.common.exception.service.exception.WebServiceException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 *
 */
public class WrappedWebApplicationException extends WebServiceException {
    protected WrappedWebApplicationException(Response.Status status, String msg, Throwable cause) {
        super(status, msg, cause);
    }

    public static WrappedWebApplicationException of(WebApplicationException ex) {
        return of(ex, ex.getMessage());
    }

    public static WrappedWebApplicationException of(WebApplicationException ex, String message) {
        Response.Status status = Response.Status.fromStatusCode(ex.getResponse().getStatus());
        return new WrappedWebApplicationException(status, message, ex);
    }
}
