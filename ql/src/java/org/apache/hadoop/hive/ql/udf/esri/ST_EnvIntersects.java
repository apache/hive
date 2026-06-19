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
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_EnvIntersects",
    value = "_FUNC_(ST_Geometry1, ST_Geometry2) - return true if the envelopes of ST_Geometry1 and ST_Geometry2 intersect",
    extended = "Example:\n"
        + "SELECT _FUNC_(ST_LineString(0,0, 1,1), ST_LineString(1,3, 2,2)) from src LIMIT 1;  -- return false\n"
        + "SELECT _FUNC_(ST_LineString(0,0, 2,2), ST_LineString(1,0, 3,2)) from src LIMIT 1;  -- return true\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_EnvIntersects(ST_LineString(0,0, 1,1), ST_LineString(1,3, 2,2)) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_EnvIntersects(ST_LineString(0,0, 2,2), ST_LineString(1,0, 3,2)) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_EnvIntersects(null, ST_LineString(0,0, 1,1)) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_EnvIntersects extends ST_Geometry {

  final BooleanWritable resultBoolean = new BooleanWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_EnvIntersects.class.getName());

  public BooleanWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
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

    Geometry geometry1 = ogcGeom1.getEsriGeometry();
    Geometry geometry2 = ogcGeom2.getEsriGeometry();
    Envelope env1 = new Envelope(), env2 = new Envelope();
    geometry1.queryEnvelope(env1);
    geometry2.queryEnvelope(env2);

    resultBoolean.set(env1.isIntersecting(env2));
    return resultBoolean;
  }

}
