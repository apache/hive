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
package org.apache.hadoop.hive.ql.udf.esri;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_AsShape",
    value = "_FUNC_(ST_Geometry) - return Esri shape representation of geometry\n",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_Point(1, 2)) FROM onerow; -- Esri shape representation of POINT (1 2)\n")
public class ST_AsShape extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_AsShape.class.getName());

  public BytesWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    Geometry jtsGeom = GeometryUtils.geometryFromEsriShape(geomref);
    if (jtsGeom == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    try {
      return new BytesWritable(EsriShapeConverter.toEsriShape(jtsGeom));
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_AsShape: " + e);
      return null;
    }
  }
}
