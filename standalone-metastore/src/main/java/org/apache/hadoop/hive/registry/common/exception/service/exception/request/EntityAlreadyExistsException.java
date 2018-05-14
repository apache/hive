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
package org.apache.hadoop.hive.registry.common.exception.service.exception.request;

import org.apache.hadoop.hive.registry.common.exception.service.exception.WebServiceException;

import javax.ws.rs.core.Response;

/**
 *
 */
public class EntityAlreadyExistsException extends WebServiceException {
    private static final String BY_ID_MESSAGE = "Entity with id [%s] already exists.";
    private static final String BY_NAME_MESSAGE = "Entity with name [%s] already exists.";

    private EntityAlreadyExistsException(String message) {
        super(Response.Status.CONFLICT, message);
    }

    private EntityAlreadyExistsException(String message, Throwable cause) {
        super(Response.Status.CONFLICT, message, cause);
    }

    public static EntityAlreadyExistsException byId(String id) {
        return new EntityAlreadyExistsException(buildMessageByID(id));
    }

    public static EntityAlreadyExistsException byId(String id, Throwable cause) {
        return new EntityAlreadyExistsException(buildMessageByID(id), cause);
    }

    public static EntityAlreadyExistsException byName(String name) {
        return new EntityAlreadyExistsException(buildMessageByName(name));
    }

    public static EntityAlreadyExistsException byName(String name, Throwable cause) {
        return new EntityAlreadyExistsException(buildMessageByName(name), cause);
    }

    private static String buildMessageByID(String id) {
        return String.format(BY_ID_MESSAGE, id);
    }

    private static String buildMessageByName(String name) {
        return String.format(BY_NAME_MESSAGE, name);
    }
}
