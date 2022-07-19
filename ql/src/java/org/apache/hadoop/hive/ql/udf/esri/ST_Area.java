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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Area",
    value = "_FUNC_(ST_Polygon) - returns the area of polygon or multipolygon",
    extended = "Example:\n" + "  SELECT _FUNC_(ST_Polygon(1,1, 1,4, 4,4, 4,1)) FROM src LIMIT 1;  --  9.0")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow",
//			result = "9.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')) from onerow",
//			result = "24.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow",
//			result = "2.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_Area extends ST_GeometryAccessor {
  final DoubleWritable resultDouble = new DoubleWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_Area.class.getName());

  public DoubleWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    resultDouble.set(ogcGeometry.getEsriGeometry().calculateArea2D());
    return resultDouble;
  }
}
