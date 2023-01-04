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

import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Centroid",
    value = "_FUNC_(geometry) - returns the centroid of the geometry",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_GeomFromText('point (2 3)'));  -- POINT(2 3)\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('multipoint ((0 0), (1 1), (1 -1), (6 0))'));  -- POINT(2 0)\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('linestring ((0 0, 6 0))'));  -- POINT(3 0)\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('linestring ((0 0, 2 4, 6 8))'));  -- POINT(3 4)\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 0 8, 8 8, 8 0, 0 0))'));  -- POINT(4 4)\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('polygon ((1 1, 5 1, 3 4))'));  -- POINT(3 2)\n")

public class ST_Centroid extends ST_GeometryAccessor {
  static final Logger LOG = LoggerFactory.getLogger(ST_PointN.class.getName());

  public BytesWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    return GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry.centroid());
  }

}
