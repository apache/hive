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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class GeoJsonSerDe extends BaseJsonSerDe {

  ObjectMapper mapper = null;

  public GeoJsonSerDe() {
    super();
    attrLabel = "properties";
    mapper = new ObjectMapper();
  }

  @Override
  protected String outGeom(OGCGeometry geom) {
    return geom.asGeoJson();
  }

  @Override
  protected OGCGeometry parseGeom(JsonParser parser) {
    try {
      ObjectNode node = mapper.readTree(parser);
      return OGCGeometry.fromGeoJson(node.toString());
    } catch (JsonProcessingException e1) {
      e1.printStackTrace();      // TODO Auto-generated catch block
    } catch (IOException e1) {
      e1.printStackTrace();      // TODO Auto-generated catch block
    }
    return null;  // ?
  }
}
