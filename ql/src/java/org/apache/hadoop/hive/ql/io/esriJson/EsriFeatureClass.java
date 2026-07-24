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
package org.apache.hadoop.hive.ql.io.esriJson;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.hadoop.hive.serde2.esriJson.deserializer.GeometryTypeJsonDeserializer;
import org.apache.hadoop.hive.serde2.esriJson.deserializer.SpatialReferenceJsonDeserializer;
import org.apache.hadoop.hive.serde2.esriJson.serializer.GeometryTypeJsonSerializer;
import org.apache.hadoop.hive.serde2.esriJson.serializer.SpatialReferenceJsonSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EsriFeatureClass {
  public String displayFieldName;

  /**
   * Map of field aliases for applicable fields in this feature class
   */
  public Map<String, Object> fieldAliases;

  /**
   * Esri geometry type string (e.g. "esriGeometryPoint", "esriGeometryPolygon").
   */
  @JsonDeserialize(using = GeometryTypeJsonDeserializer.class)
  @JsonSerialize(using = GeometryTypeJsonSerializer.class)
  public String geometryType;

  /**
   * Spatial reference WKID for the feature class (0 if undefined).
   * The JSON form is {"wkid": N}; deserialized to a plain int.
   */
  @JsonDeserialize(using = SpatialReferenceJsonDeserializer.class)
  @JsonSerialize(using = SpatialReferenceJsonSerializer.class)
  public int spatialReference;

  /**
   * Array of field definitions (name, type, alias, ...)
   */
  public EsriField[] fields;

  /**
   * Array of features (attributes, geometry)
   */
  public EsriFeature[] features;

  /**
   *
   * @param jsonStream JSON input stream
   * @return EsriFeatureClass instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeatureClass fromJson(InputStream jsonStream) throws JsonParseException, IOException {
    return EsriJsonFactory.FeatureClassFromJson(jsonStream);
  }

  /**
   *
   * @param parser parser that is pointed at the root of the JSON file created by ArcGIS
   * @return EsriFeatureClass instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeatureClass fromJson(JsonParser parser) throws JsonParseException, IOException {
    return EsriJsonFactory.FeatureClassFromJson(parser);
  }

  /**
   *
   * @return JSON string representation of this feature class
   * @throws com.fasterxml.jackson.core.JsonGenerationException
   * @throws com.fasterxml.jackson.databind.JsonMappingException
   * @throws java.io.IOException
   */
  public String toJson() throws JsonGenerationException, JsonMappingException, IOException {
    return EsriJsonFactory.JsonFromFeatureClass(this);
  }
}
