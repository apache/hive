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
package org.apache.hcatalog.templeton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Helper class to build new json objects with new top level
 * properties.  Only add non-null entries.
 */
public class JsonBuilder {
    static final int OK = 200;
    static final int MISSING = 404;
    static final int SERVER_ERROR = 500;

    // The map we're building.
    private Map map;

    // Parse the json map.
    private JsonBuilder(String json)
        throws IOException {
        map = jsonToMap(json);
    }

    /**
     * Create a new map object from the existing json.
     */
    public static JsonBuilder create(String json)
        throws IOException {
        return new JsonBuilder(json);
    }

    /**
     * Create a new map object.
     */
    public static JsonBuilder create()
        throws IOException {
        return new JsonBuilder(null);
    }

    /**
     * Create a new map error object.
     */
    public static JsonBuilder createError(String msg, int code)
        throws IOException {
        return new JsonBuilder(null)
            .put("error", msg)
            .put("errorCode", code);
    }

    /**
     * Add a non-null value to the map.
     */
    public JsonBuilder put(String name, Object val) {
        if (val != null)
            map.put(name, val);
        return this;
    }

    /**
     * Remove a value from the map.
     */
    public JsonBuilder remove(String name) {
        map.remove(name);
        return this;
    }

    /**
     * Get the underlying map.
     */
    public Map getMap() {
        return map;
    }

    /**
     * Turn the map back to response object.
     */
    public Response build() {
        return buildResponse();
    }

    /**
     * Turn the map back to json.
     */
    public String buildJson()
        throws IOException {
        return mapToJson(map);
    }

    /**
     * Turn the map back to response object.
     */
    public Response buildResponse() {
        int status = OK;        // Server ok.
        if (map.containsKey("error"))
            status = SERVER_ERROR; // Generic http server error.
        Object o = map.get("errorCode");
        if (o != null) {
            try {
                status = Integer.parseInt(o.toString());
            } catch (Exception e) {
                if (o instanceof Number)
                    status = ((Number) o).intValue();
            }
        }
        return buildResponse(status);
    }

    /**
     * Turn the map back to response object.
     */
    public Response buildResponse(int status) {
        return Response.status(status)
            .entity(map)
            .type(MediaType.APPLICATION_JSON)
            .build();
    }

    /**
     * Is the object non-empty?
     */
    public boolean isset() {
        return TempletonUtils.isset(map);
    }

    /**
     * Check if this is an error doc.
     */
    public static boolean isError(Map obj) {
        return (obj != null) && obj.containsKey("error");
    }

    /**
     * Convert a json string to a Map.
     */
    public static Map jsonToMap(String json)
        throws IOException {
        if (!TempletonUtils.isset(json))
            return new HashMap<String, Object>();
        else {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, Map.class);
        }
    }

    /**
     * Convert a map to a json string.
     */
    public static String mapToJson(Object obj)
        throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, obj);
        return out.toString();
    }
}
