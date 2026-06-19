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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.udf.esri.GeometryUtils.OGCType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Union",
    value = "_FUNC_(ST_Geometry, ST_Geometry, ...) - returns an ST_Geometry as the union of the supplied ST_Geometries",
    extended =
        "Example: SELECT ST_AsText(ST_Union(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), ST_Polygon(4, 1, 4, 4, 4, 8, 8, 1))) FROM onerow;\n"
            + "MULTIPOLYGON (((4 1, 8 1, 4 8, 4 4, 1 4, 1 1, 4 1)))")
//@HivePdkUnitTests(
//		cases = {
//				@HivePdkUnitTest(
//						query = "SELECT ST_AsText(ST_Union(ST_Point(1.1, 2.2), ST_Point(3.3, 4.4))) FROM onerow",
//						result = "MULTIPOINT (1.1 2.2, 3.3 4.4)"
//						),
//				@HivePdkUnitTest(
//						query = "SELECT ST_AsText(ST_Union(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), ST_Polygon(4,1, 4,8, 8,1))) FROM onerow",
//						result = "MULTIPOLYGON (((4 1, 8 1, 4 8, 4 4, 1 4, 1 1, 4 1)))"
//						),
//				@HivePdkUnitTest(
//						query = "SELECT ST_AsText(ST_Union(ST_Point(1.1, 2.2), ST_Point(3.3, 4.4), ST_Point(5.5, 6.6), ST_Point(1.1, 2.2))) FROM onerow",
//						result = "MULTIPOINT (1.1 2.2, 3.3 4.4, 5.5 6.6)"
//						)
//			}
//		)
public class ST_Union extends ST_GeometryProcessing {
  static final Logger LOG = LoggerFactory.getLogger(ST_Union.class.getName());

  public BytesWritable evaluate(BytesWritable... geomrefs) {
    // validate arguments
    if (geomrefs == null || geomrefs.length < 2) {
      LogUtils.Log_VariableArgumentLength(LOG);
    }

    int firstWKID = 0;

    SpatialReference spatialRef = null;

    // validate spatial references and geometries first
    for (int i = 0; i < geomrefs.length; i++) {

      BytesWritable geomref = geomrefs[i];

      if (geomref == null || geomref.getLength() == 0) {
        LogUtils.Log_ArgumentsNull(LOG);
        return null;
      }

      if (i == 0) {
        firstWKID = GeometryUtils.getWKID(geomref);
        if (firstWKID != GeometryUtils.WKID_UNKNOWN) {
          spatialRef = SpatialReference.create(firstWKID);
        }
      } else if (firstWKID != GeometryUtils.getWKID(geomref)) {
        LogUtils.Log_SRIDMismatch(LOG, geomrefs[0], geomref);
        return null;
      }
    }

    // now build geometry array to pass to GeometryEngine.union
    Geometry[] geomsToUnion = new Geometry[geomrefs.length];

    for (int i = 0; i < geomrefs.length; i++) {
      //HiveGeometry hiveGeometry = GeometryUtils.geometryFromEsriShape(geomrefs[i]);
      OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomrefs[i]);

      // if (i==0){   // get from ogcGeometry rather than re-create above?
      // 	spatialRef = hiveGeometry.spatialReference;
      // }

      if (ogcGeometry == null) {
        LogUtils.Log_ArgumentsNull(LOG);
        return null;
      }

      geomsToUnion[i] = ogcGeometry.getEsriGeometry();
    }

    try {
      Geometry unioned = GeometryEngine.union(geomsToUnion, spatialRef);

      // we have to infer the type of the differenced geometry because we don't know
      // if it's going to end up as a single or multi-part geometry
      OGCType inferredType = GeometryUtils.getInferredOGCType(unioned);

      return GeometryUtils.geometryToEsriShapeBytesWritable(unioned, firstWKID, inferredType);
    } catch (Exception e) {
      LogUtils.Log_ExceptionThrown(LOG, "GeometryEngine.union", e);
      return null;
    }
  }
}
