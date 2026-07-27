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
package org.apache.hadoop.hive.serde2.esriJson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 *
 * Deserializes a JSON spatial reference definition into an integer SRID (WKID).
 * Reads the "wkid" field from the spatialReference JSON object.
 * Returns 0 if the spatial reference is absent or cannot be parsed.
 */
public class SpatialReferenceJsonDeserializer extends JsonDeserializer<Integer> {

  public SpatialReferenceJsonDeserializer() {
  }

  @Override
  public Integer deserialize(JsonParser parser, DeserializationContext arg1)
      throws IOException, JsonProcessingException {
    try {
      // The parser is currently positioned at the START_OBJECT token for spatialReference.
      // Walk through the object looking for the "wkid" field.
      int wkid = 0;
      JsonToken token = parser.getCurrentToken();
      if (token == JsonToken.START_OBJECT) {
        token = parser.nextToken();
      }
      while (token != null && token != JsonToken.END_OBJECT) {
        if (token == JsonToken.FIELD_NAME) {
          String fieldName = parser.getCurrentName();
          parser.nextToken();
          if ("wkid".equalsIgnoreCase(fieldName) || "latestWkid".equalsIgnoreCase(fieldName)) {
            wkid = parser.getIntValue();
          }
        }
        token = parser.nextToken();
      }
      return wkid;
    } catch (Exception e) {
      return 0;
    }
  }
}
