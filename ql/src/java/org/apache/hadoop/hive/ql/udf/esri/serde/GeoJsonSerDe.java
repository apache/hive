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
package org.apache.hadoop.hive.ql.udf.esri.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

public class GeoJsonSerDe extends BaseJsonSerDe {

  private final ObjectMapper mapper = new ObjectMapper();

  public GeoJsonSerDe() {
    super("properties");
  }

  @Override
  protected String outGeom(Geometry geom) {
    return GeometryUtils.geoJsonWriter().write(geom);
  }

  @Override
  protected Geometry parseGeom(JsonParser parser) {
    try {
      ObjectNode node = mapper.readTree(parser);
      return GeometryUtils.geoJsonReader().read(node.toString());
    } catch (ParseException | IOException e) {
      LOG.error("Error parsing GeoJSON", e);
    }
    return null;
  }
}
