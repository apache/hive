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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.udf.esri.EsriJsonConverter;
import org.locationtech.jts.geom.Geometry;

public class EsriJsonSerDe extends BaseJsonSerDe {

  @Override
  protected String outGeom(Geometry geom) {
    try {
      int wkid = geom.getSRID();
      return EsriJsonConverter.geometryToEsriJson(geom, wkid);
    } catch (Exception e) {
      return "null";
    }
  }

  @Override
  protected Geometry parseGeom(JsonParser parser) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(parser);
      if (node == null) {
        return null;
      }
      return EsriJsonConverter.esriJsonToGeometry(node.toString());
    } catch (Exception e) {
      return null;
    }
  }
}
