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
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_AsGeoJSON",
    value = "_FUNC_(geometry) - return GeoJSON representation of geometry\n",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_Point(1.0, 2.0)) from onerow; -- {\"type\":\"Point\", \"coordinates\":[1.0, 2.0]}\n"
        + "Note : \n" + " ST_AsGeoJSON outputs the _geometry_ contents but not _crs_.\n"
        + " ST_AsGeoJSON requires geometry-api-java version 1.1 or later.\n")
//@HivePdkUnitTests(
//	cases = { 
//		@HivePdkUnitTest(
//			query = "select ST_AsGeoJSON(ST_point(1, 2))) from onerow",
//			result = "{\"type\":\"Point\", \"coordinates\":[1.0, 2.0]}"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsGeoJSON(ST_MultiLineString(array(1, 1, 2, 2, 3, 3), array(7,7, 8,8, 9,9))) from onerow",
//			result = "{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,1.0],[2.0,2.0],[3.0,3.0]],[[7.0,7.0],[8.0,8.0],[9.0,9.0]]]}"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsGeoJSON(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow",
//			result = "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[1.0,4.0],[4.0,4.0],[4.0,1.0],[1.0,1.0]]]}"
//			)
//		}
//	)

public class ST_AsGeoJson extends ST_Geometry {
  final Text resultText = new Text();
  static final Logger LOG = LoggerFactory.getLogger(ST_AsGeoJson.class.getName());

  public Text evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    try {
      String outJson = ogcGeometry.asGeoJson();
      resultText.set(outJson);
      return resultText;
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_AsGeoJSON: " + e);
      return null;
    }
  }

}
