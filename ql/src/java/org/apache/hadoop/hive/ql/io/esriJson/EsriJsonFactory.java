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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.hive.serde2.esriJson.deserializer.GeometryJsonDeserializer;
import org.apache.hadoop.hive.serde2.esriJson.deserializer.GeometryTypeJsonDeserializer;
import org.apache.hadoop.hive.serde2.esriJson.deserializer.SpatialReferenceJsonDeserializer;
import org.apache.hadoop.hive.serde2.esriJson.serializer.GeometryJsonSerializer;
import org.apache.hadoop.hive.serde2.esriJson.serializer.GeometryTypeJsonSerializer;
import org.apache.hadoop.hive.serde2.esriJson.serializer.SpatialReferenceJsonSerializer;

import java.io.IOException;
import java.io.InputStream;

public class EsriJsonFactory {

  private static final ObjectMapper jsonObjectMapper;
  private static final JsonFactory jsonFactory = new JsonFactory();

  static {
    jsonObjectMapper = new ObjectMapper();

    SimpleModule module = new SimpleModule("EsriJsonModule", new Version(1, 0, 0, null));

    // add deserializers and serializers for types that can't be mapped field for field from the JSON
    module.addDeserializer(Geometry.class, new GeometryJsonDeserializer());
    module.addDeserializer(SpatialReference.class, new SpatialReferenceJsonDeserializer());
    module.addDeserializer(Geometry.Type.class, new GeometryTypeJsonDeserializer());

    module.addSerializer(Geometry.class, new GeometryJsonSerializer());
    module.addSerializer(Geometry.Type.class, new GeometryTypeJsonSerializer());
    module.addSerializer(SpatialReference.class, new SpatialReferenceJsonSerializer());

    jsonObjectMapper.registerModule(module);
  }

  private EsriJsonFactory() { /* disable instance creation */ }

  /**
   * Create JSON from an {@link EsriFeatureClass}
   *
   * @param featureClass feature class to convert to JSON
   * @return JSON string representing the given feature class
   * @throws com.fasterxml.jackson.core.JsonGenerationException
   * @throws com.fasterxml.jackson.databind.JsonMappingException
   * @throws java.io.IOException
   */
  public static String JsonFromFeatureClass(EsriFeatureClass featureClass)
      throws JsonGenerationException, JsonMappingException, IOException {
    return jsonObjectMapper.writeValueAsString(featureClass);
  }

  /**
   * Construct an {@link EsriFeatureClass} from JSON
   *
   * @param jsonInputStream JSON input stream
   * @return EsriFeatureClass instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeatureClass FeatureClassFromJson(InputStream jsonInputStream)
      throws JsonParseException, IOException {
    JsonParser parser = jsonFactory.createJsonParser(jsonInputStream);
    return FeatureClassFromJson(parser);
  }

  /**
   * Construct an {@link EsriFeatureClass} from JSON
   *
   * @param parser parser that is pointed at the root of the JSON file created by ArcGIS
   * @return EsriFeatureClass instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeatureClass FeatureClassFromJson(JsonParser parser) throws JsonProcessingException, IOException {
    parser.setCodec(jsonObjectMapper);
    return parser.readValueAs(EsriFeatureClass.class);
  }

  /**
   * Create JSON from an {@link EsriFeature}
   *
   * @param feature feature to convert to JSON
   * @return JSON string representing the given feature
   * @throws com.fasterxml.jackson.core.JsonGenerationException
   * @throws com.fasterxml.jackson.databind.JsonMappingException
   * @throws java.io.IOException
   */
  public static String JsonFromFeature(EsriFeature feature)
      throws JsonGenerationException, JsonMappingException, IOException {
    return jsonObjectMapper.writeValueAsString(feature);
  }

  /**
   * Construct an {@link EsriFeature} from JSON
   *
   * @param jsonInputStream JSON input stream
   * @return EsriFeature instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeature FeatureFromJson(InputStream jsonInputStream) throws JsonParseException, IOException {
    JsonParser parser = jsonFactory.createJsonParser(jsonInputStream);
    return FeatureFromJson(parser);
  }

  /**
   * Construct an {@link EsriFeature} from JSON
   *
   * @param parser parser that is pointed at the root of the JSON file created by ArcGIS
   * @return EsriFeature instance that describes the fully parsed JSON representation
   * @throws com.fasterxml.jackson.core.JsonParseException
   * @throws java.io.IOException
   */
  public static EsriFeature FeatureFromJson(JsonParser parser) throws JsonProcessingException, IOException {
    parser.setCodec(jsonObjectMapper);
    return parser.readValueAs(EsriFeature.class);
  }

}
