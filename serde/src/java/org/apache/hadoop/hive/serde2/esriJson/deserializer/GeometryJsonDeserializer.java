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

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;

import java.io.IOException;

/**
 *
 * Deserializes a JSON geometry definition into a JTS Geometry instance.
 * Uses the ESRI API to parse Esri JSON, then converts to JTS via WKB round-trip.
 */
public class GeometryJsonDeserializer extends JsonDeserializer<Geometry> {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public GeometryJsonDeserializer() {
  }

  @Override
  public Geometry deserialize(JsonParser arg0, DeserializationContext arg1)
      throws IOException, JsonProcessingException {
    try {
      MapGeometry mapGeom = GeometryEngine.jsonToGeometry(arg0);
      if (mapGeom == null || mapGeom.getGeometry() == null) {
        return null;
      }
      OGCGeometry ogcGeom = OGCGeometry.createFromEsriGeometry(
          mapGeom.getGeometry(), mapGeom.getSpatialReference());
      java.nio.ByteBuffer wkbBuf = ogcGeom.asBinary();
      byte[] wkbBytes = new byte[wkbBuf.remaining()];
      wkbBuf.get(wkbBytes);
      Geometry jtsGeom = new WKBReader(GEOMETRY_FACTORY).read(wkbBytes);
      if (mapGeom.getSpatialReference() != null) {
        jtsGeom.setSRID(mapGeom.getSpatialReference().getID());
      }
      return jtsGeom;
    } catch (Exception e) {
      return null;
    }
  }
}
