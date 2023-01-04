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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Relate",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - return true if ST_Geometry1 has the specified DE-9IM relationship with ST_Geometry2",
    extended = "Example:\n"
        + "  SELECT _FUNC_(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1), '****T****') from src LIMIT 1;  -- true\n"
        + "  SELECT _FUNC_(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1), 'T********') from src LIMIT 1;  -- false\n"
        + "  SELECT _FUNC_(st_linestring(0,0, 3,3), ST_linestring(1,1, 4,4), 'T********') from src LIMIT 1;  -- true\n"
        + "  SELECT _FUNC_(st_linestring(0,0, 3,3), ST_linestring(1,1, 4,4), '****T****') from src LIMIT 1;  -- false\n")

public class ST_Relate extends ST_Geometry {

  final BooleanWritable resultBoolean = new BooleanWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_Relate.class.getName());

  public BooleanWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2, String relation) {
    if (geometryref1 == null || geometryref2 == null || relation == null || geometryref1.getLength() == 0
        || geometryref2.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }
    if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
      LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
      return null;
    }

    OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
    OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
    if (ogcGeom1 == null || ogcGeom2 == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    try {
      resultBoolean.set(ogcGeom1.relate(ogcGeom2, relation));
      return resultBoolean;
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_Relate: " + e);
      return null;
    }
  }

}
