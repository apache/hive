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

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

import java.io.IOException;

/**
 * Serializes a JTS Geometry to Esri JSON format.
 * Converts JTS -> ESRI via WKB round-trip, then uses GeometryEngine.geometryToJson().
 */
public class GeometryJsonSerializer extends JsonSerializer<Geometry> {

  @Override
  public void serialize(Geometry geometry, JsonGenerator jsonGenerator, SerializerProvider arg2) throws IOException {
    try {
      byte[] wkb = new WKBWriter().write(geometry);
      OGCGeometry ogcGeom = OGCGeometry.fromBinary(java.nio.ByteBuffer.wrap(wkb));
      com.esri.core.geometry.Geometry esriGeom = ogcGeom.getEsriGeometry();
      int wkid = geometry.getSRID();
      jsonGenerator.writeRawValue(GeometryEngine.geometryToJson(wkid, esriGeom));
    } catch (Exception e) {
      jsonGenerator.writeNull();
    }
  }
}
