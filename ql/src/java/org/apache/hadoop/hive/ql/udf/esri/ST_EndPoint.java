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

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_EndPoint",
    value = "_FUNC_(geometry) - returns the last point of an ST_Linestring",
    extended = "Example:\n"
        + "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- POINT(3.0 2.0)\n")

public class ST_EndPoint extends ST_GeometryAccessor {
  static final Logger LOG = LoggerFactory.getLogger(ST_EndPoint.class.getName());

  /**
   * Return the last point of the ST_Linestring.
   * @param geomref hive geometry bytes
   * @return byte-reference of the last ST_Point
   */
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

    if (GeometryUtils.getType(geomref) == GeometryUtils.OGCType.ST_LINESTRING) {
      MultiPath lines = (MultiPath) (ogcGeometry.getEsriGeometry());
      int wkid = GeometryUtils.getWKID(geomref);
      SpatialReference spatialReference = null;
      if (wkid != GeometryUtils.WKID_UNKNOWN) {
        spatialReference = SpatialReference.create(wkid);
      }
      return GeometryUtils.geometryToEsriShapeBytesWritable(
          OGCGeometry.createFromEsriGeometry(lines.getPoint(lines.getPointCount() - 1), spatialReference));
    } else {
      LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_LINESTRING, GeometryUtils.getType(geomref));
      return null;
    }
  }
}
