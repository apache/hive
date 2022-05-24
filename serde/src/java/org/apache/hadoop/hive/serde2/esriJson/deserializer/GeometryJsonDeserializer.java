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
import com.esri.core.geometry.GeometryEngine;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 *
 * Deserializes a JSON geometry definition into a Geometry instance
 */
public class GeometryJsonDeserializer extends JsonDeserializer<Geometry> {

  public GeometryJsonDeserializer() {
  }

  @Override
  public Geometry deserialize(JsonParser arg0, DeserializationContext arg1)
      throws IOException, JsonProcessingException {
    return GeometryEngine.jsonToGeometry(arg0).getGeometry();
  }
}
