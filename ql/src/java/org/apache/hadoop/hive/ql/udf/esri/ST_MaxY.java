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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_MaxY",
    value = "_FUNC_(geometry) - returns the maximum Y coordinate of geometry",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 2.5\n"
        + "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- 2.5\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_Point(1,2)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow",
//			result = "2.5"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_MultiPoint(0,0, 4,2)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 25, 20))) from onerow",
//			result = "20"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxY(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_MaxY extends ST_GeometryAccessor {
  final DoubleWritable resultDouble = new DoubleWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_MaxY.class.getName());

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

    Envelope envBound = new Envelope();
    ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
    resultDouble.set(envBound.getYMax());
    return resultDouble;
  }
}
