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

import com.esri.core.geometry.Geometry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 *
 * Deserializes a JSON geometry type enumeration into a Geometry.Type.* enumeration
 */
public class GeometryTypeJsonDeserializer extends JsonDeserializer<Geometry.Type> {

  public GeometryTypeJsonDeserializer() {
  }

  @Override
  public Geometry.Type deserialize(JsonParser parser, DeserializationContext arg1)
      throws IOException, JsonProcessingException {

    String type_text = parser.getText();

    // geometry type enumerations coming from the JSON are prepended with "esriGeometry" (i.e. esriGeometryPolygon)
    // while the geometry-java-api uses the form Geometry.Type.Polygon
    if (type_text.startsWith("esriGeometry")) {
      // cut out esriGeometry to match Geometry.Type enumeration values
      type_text = type_text.substring(12);

      try {
        return Enum.valueOf(Geometry.Type.class, type_text);
      } catch (Exception e) {
        // parsing failed, fall through to unknown geometry type
      }
    }

    return Geometry.Type.Unknown;
  }
}
