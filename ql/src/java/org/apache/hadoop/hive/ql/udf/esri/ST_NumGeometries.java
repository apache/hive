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
import org.apache.hadoop.io.IntWritable;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_NumGeometries",
    value = "_FUNC_(ST_GeometryCollection) - return the number of geometries in the geometry collection",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_GeomFromText('multipoint ((10 40), (40 30), (20 20), (30 10))')) FROM src LIMIT 1;  -- 4\n"
        + "  SELECT _FUNC_(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) FROM src LIMIT 1;  -- 2\n")

public class ST_NumGeometries extends ST_GeometryAccessor {
  final IntWritable resultInt = new IntWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_NumGeometries.class.getName());

  public IntWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    Geometry geom = GeometryUtils.geometryFromEsriShape(geomref);
    if (geom == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    try {
      GeometryUtils.OGCType ogcType = GeometryUtils.getType(geomref);
      switch (ogcType) {
      case ST_POINT:
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_MULTIPOINT, ogcType);
        return null;
      case ST_LINESTRING:
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_MULTILINESTRING, ogcType);
        return null;
      case ST_POLYGON:
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_MULTIPOLYGON, ogcType);
        return null;
      default:
        resultInt.set(geom.getNumGeometries());
        break;
      }
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_NumGeometries: " + e);
      return null;
    }
    return resultInt;
  }

}
