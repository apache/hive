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
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Envelope",
    value = "_FUNC_(ST_Geometry) - the envelope of the ST_Geometry",
    extended = "Example:\n"
        + "SELECT _FUNC_(ST_LineString(0,0, 2,2)) from src LIMIT 1;  -- POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))\n"
        + "SELECT _FUNC_(ST_Polygon(2,0, 2,3, 3,0)) from src LIMIT 1;  -- POLYGON ((2 0, 3 0, 3 3, 2 3, 2 0))\n"
        + "OGC Compliance Notes : \n" + " In the case of a point or a vertical or horizontal line,"
        + " ST_Envelope may either apply a tolerance or return an empty envelope.")
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

public class ST_Envelope extends ST_GeometryProcessing {
  static final Logger LOG = LoggerFactory.getLogger(ST_Envelope.class.getName());

  public BytesWritable evaluate(BytesWritable geometryref) {
    if (geometryref == null || geometryref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryref);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    int wkid = GeometryUtils.getWKID(geometryref);
    SpatialReference spatialReference = null;
    if (wkid != GeometryUtils.WKID_UNKNOWN) {
      spatialReference = SpatialReference.create(wkid);
    }
    Envelope envBound = new Envelope();
    ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
    return GeometryUtils
        .geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(envBound, spatialReference));
  }

}
