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

@Description(name = "ST_Difference",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - return the difference of ST_Geometry1 - ST_Geometry2",
    extended = "Examples:\n"
        + " > SELECT ST_AsText(ST_Difference(ST_MultiPoint(1, 1, 1.5, 1.5, 2, 2), ST_Point(1.5, 1.5))) FROM onerow; \n"
        + " MULTIPOINT (1 1, 2 2)\n"
        + " > SELECT ST_AsText(ST_Difference(ST_Polygon(0, 0, 0, 10, 10, 10, 10, 0), ST_Polygon(0, 0, 0, 5, 5, 5, 5, 0))) from onerow;\n"
        + " MULTIPOLYGON (((10 0, 10 10, 0 10, 0 5, 5 5, 5 0, 10 0)))\n\n")
//@HivePdkUnitTests(
//		cases = {
//				@HivePdkUnitTest(
//						query = "SELECT ST_AsText(ST_Difference(ST_MultiPoint(1, 1, 1.5, 1.5, 2, 2), ST_Point(1.5, 1.5))) FROM onerow",
//						result = "MULTIPOINT (1 1, 2 2)"
//						),
//				@HivePdkUnitTest(
//						query = "SELECT ST_AsText(ST_Difference(ST_Polygon(0, 0, 0, 10, 10, 10, 10, 0), ST_Polygon(0, 0, 0, 5, 5, 5, 5, 0))) from onerow",
//						result = "MULTIPOLYGON (((10 0, 10 10, 0 10, 0 5, 5 5, 5 0, 10 0)))"
//						)
//			}
//		)
public class ST_Difference extends ST_GeometryProcessing {

  static final Logger LOG = LoggerFactory.getLogger(ST_Difference.class.getName());

  public BytesWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
    if (geometryref1 == null || geometryref2 == null || geometryref1.getLength() == 0
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

    OGCGeometry diffGeometry = ogcGeom1.difference(ogcGeom2);

    // we have to infer the type of the differenced geometry because we don't know
    // if it's going to end up as a single or multi-part geometry
    // OGCType inferredType = GeometryUtils.getInferredOGCType(diffGeometry.getEsriGeometry());

    return GeometryUtils.geometryToEsriShapeBytesWritable(diffGeometry);
  }
}
