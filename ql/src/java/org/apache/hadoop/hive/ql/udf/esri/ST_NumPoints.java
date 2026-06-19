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
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_NumPoints",
    value = "_FUNC_(geometry) - return the number of points in the geometry",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 1\n"
        + "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- 2\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 10 0, 0 10, 0 0))')) FROM src LIMIT 1;  -- 4\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_Point(0., 3.)) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_LineString(0.,0., 3.,4.)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_GeomFromText('polygon ((0 0, 10 0, 0 10, 0 0))')) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_GeomFromText('multipoint ((10 40), (40 30), (20 20), (30 10))', 0)) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumPoints(ST_Point('point empty')) from onerow",
//			result = "0"
//			)
//		}
//	)

public class ST_NumPoints extends ST_GeometryAccessor {
  final IntWritable resultInt = new IntWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_IsClosed.class.getName());

  public IntWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    Geometry esriGeom = ogcGeometry.getEsriGeometry();
    switch (esriGeom.getType()) {
    case Point:
      resultInt.set(esriGeom.isEmpty() ? 0 : 1);
      break;
    case MultiPoint:
      resultInt.set(((MultiPoint) (esriGeom)).getPointCount());
      break;
    case Polygon:
      Polygon polygon = (Polygon) (esriGeom);
      resultInt.set(polygon.getPointCount() + polygon.getPathCount());
      break;
    default:
      resultInt.set(((MultiPath) (esriGeom)).getPointCount());
      break;
    }
    return resultInt;
  }
}
