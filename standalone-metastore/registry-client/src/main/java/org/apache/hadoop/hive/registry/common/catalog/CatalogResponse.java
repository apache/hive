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

package org.apache.hadoop.hive.registry.common.catalog;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;


/**
 * <p>
 * A wrapper entity for passing entities and status back to the client.
 * </p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CatalogResponse {

    /**
     * ResponseMessage args if any should always be string to keep it simple.
     */
    public enum ResponseMessage {
        /* 1000 to 1100 reserved for success status messages */
        SUCCESS(1000, "Success", 0),
        /* 1101 onwards for error messages */
        ENTITY_NOT_FOUND(1101, "Entity with id [%s] not found.", 1),
        EXCEPTION(1102, "An exception was thrown while processing request with message: [%s]", 1),
        BAD_REQUEST_PARAM_MISSING(1103, "Bad request - %s.", 1),
        DATASOURCE_TYPE_FILTER_NOT_FOUND(1104, "Datasource not found for type [%s], query params [%s].", 2),
        ENTITY_NOT_FOUND_FOR_FILTER(1105, "Entity not found for query params [%s].", 1),
        INCOMPATIBLE_SCHEMA(1106, "%s", 1),
        INVALID_SCHEMA(1107, "Given schema is invalid. %s", 1),
        UNSUPPORTED_SCHEMA_TYPE(1108, "Given schema type is not supported.", 0),
        UNSUPPORTED_MEDIA_TYPE(1109, "Unsupported Media Type.", 0),
        BAD_REQUEST(1110, "Bad Request.", 0),
        ENTITY_CONFLICT(1111, "An entity with id [%s] already exists",1),
        BAD_REQUEST_WITH_MESSAGE(1112, "Bad Request - %s",1);

        private final int code;
        private final String msg;
        private final int nargs;

        ResponseMessage(int code, String msg, int nargs) {
            this.code = code;
            this.msg = msg;
            this.nargs = nargs;
        }

        /*
         * whether an error message or just a status.
         */
        private boolean isError() {
            return code > 1100;
        }

        public int getCode() {
            return code;
        }

        public static String format(ResponseMessage responseMessage, String... args) {
            //TODO: validate number of args
            return String.format(responseMessage.msg, args);
        }

    }

    /**
     * Response code.
     */
    private int responseCode;
    /**
     * Response message.
     */
    private String responseMessage;
    /**
     * For response that returns a single entity.
     */
    private Object entity;
    /**
     * For response that returns a collection of entities.
     */
    private Collection<?> entities;

    private CatalogResponse() {
    }

    public static class Builder {
        private final ResponseMessage responseMessage;
        private Object entity;
        private Collection<?> entities;
        private final String DOC_LINK_MESSAGE = " Please check webservice/ErrorCodes.md for more details.";

        public Builder(ResponseMessage responseMessage) {
            this.responseMessage = responseMessage;
        }

        public Builder entity(Object entity) {
            this.entity = entity;
            return this;
        }

        public Builder entities(Collection<?> entities) {
            this.entities = entities;
            return this;
        }

        public CatalogResponse format(String... args) {
            CatalogResponse response = new CatalogResponse();
            response.responseCode = responseMessage.code;
            response.responseMessage = ResponseMessage.format(responseMessage, args);
            response.entity = entity;
            response.entities = entities;
            return response;
        }
    }

    public static Builder newResponse(ResponseMessage msg) {
        return new Builder(msg);
    }

    public String getResponseMessage() {
        return responseMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public Object getEntity() {
        return entity;
    }

    public Collection<?> getEntities() {
        return entities;
    }

}
