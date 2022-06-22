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

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_MultiPoint",
    value = "_FUNC_(x1, y1, x2, y2, x3, y3) - constructor for 2D multipoint\n"
        + "_FUNC_('multipoint( ... )') - constructor for 2D multipoint",
    extended = "Example:\n" + "  SELECT _FUNC_(1, 1, 2, 2, 3, 3) from src LIMIT 1; -- multipoint with 3 points\n"
        + "  SELECT _FUNC_('MULTIPOINT ((10 40), (40 30))') from src LIMIT 1; -- multipoint of 2 points")
//@HivePdkUnitTests(
//	cases = { 
//		@HivePdkUnitTest(
//			query = "select st_asjson(st_multipoint(1, 1, 2, 2, 3, 3)) from onerow",
//			result = "{\"points\":[[1.0,1.0],[2.0,2.0],[3.0,3.0]]}"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_MultiPoint('MULTIPOINT ((10 40), (40 30))')) from onerow",
//			result = "ST_MULTIPOINT"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_MultiPoint('MULTIPOINT ((10 40), (40 30))'), ST_GeomFromText('MULTIPOINT ((10 40), (40 30))')) from onerow",
//			result = "true"
//			)
//	}
//)

public class ST_MultiPoint extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_MultiPoint.class.getName());

  // Number-pairs constructor
  public BytesWritable evaluate(DoubleWritable... xyPairs) throws UDFArgumentLengthException {

    if (xyPairs == null || xyPairs.length == 0 || xyPairs.length % 2 != 0) {
      LogUtils.Log_VariableArgumentLengthXY(LOG);
      return null;
    }

    try {
      MultiPoint mPoint = new MultiPoint();

      for (int i = 0; i < xyPairs.length; i += 2) {
        mPoint.add(xyPairs[i].get(), xyPairs[i + 1].get());
      }

      return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(mPoint, null, true));
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_MultiPoint: " + e);
      return null;
    }
  }

  // WKT constructor - can use SetSRID on constructed multi-point
  public BytesWritable evaluate(Text wkwrap) throws UDFArgumentException {
    String wkt = wkwrap.toString();
    try {
      OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
      ogcObj.setSpatialReference(null);
      if (ogcObj.geometryType().equals("MultiPoint")) {
        return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
      } else {
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_MULTIPOINT, GeometryUtils.OGCType.UNKNOWN);
        return null;
      }
    } catch (Exception e) {  // IllegalArgumentException, GeometryException
      LogUtils.Log_InvalidText(LOG, wkt);
      return null;
    }
  }

}
