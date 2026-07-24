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
package org.apache.hadoop.hive.serde2.esriJson.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializes a geometry type String (e.g. "esriGeometryPolygon") to JSON.
 * The String is already in the Esri "esriGeometry*" form as produced by
 * GeometryTypeJsonDeserializer; it is written verbatim.
 */
public class GeometryTypeJsonSerializer extends JsonSerializer<String> {

  /** Prefix of every Esri geometry type name, e.g. "esriGeometryPolygon". */
  public static final String GEOMETRY_TYPE_PREFIX = "esriGeometry";

  /** Esri geometry type name used when the type is absent or not recognized. */
  public static final String GEOMETRY_TYPE_UNKNOWN = GEOMETRY_TYPE_PREFIX + "Unknown";

  @Override
  public void serialize(String geometryType, JsonGenerator jsonGenerator, SerializerProvider arg2)
      throws IOException, JsonProcessingException {
    if (geometryType == null) {
      jsonGenerator.writeNull();
    } else if (geometryType.startsWith(GEOMETRY_TYPE_PREFIX)) {
      // Already in canonical form — write as-is
      jsonGenerator.writeString(geometryType);
    } else {
      // Normalise bare type names (e.g. "Polygon" -> "esriGeometryPolygon")
      jsonGenerator.writeString(GEOMETRY_TYPE_PREFIX + geometryType);
    }
  }
}
