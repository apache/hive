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

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

@Description(name = "ST_PolyFromWKB",
    value = "_FUNC_(wkb) - construct an ST_Polygon from OGC well-known binary",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_AsBinary(ST_GeomFromText('polygon ((0 0, 10 0, 0 10, 0 0))'))) FROM src LIMIT 1;  -- constructs ST_Polygon\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_PolyFromWKB(ST_AsBinary(ST_GeomFromText('polygon ((0 0, 1 0, 0 1, 0 0))')))) from onerow",
//			result = "ST_POLYGON"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_PolyFromWKB(ST_AsBinary(ST_GeomFromText('polygon ((0 0, 1 0, 0 1, 0 0))'))), ST_GeomFromText('polygon ((0 0, 1 0, 0 1, 0 0))')) from onerow",
//			result = "true"
//			)
//		}
//	)

public class ST_PolyFromWKB extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_PolyFromWKB.class.getName());

  public BytesWritable evaluate(BytesWritable wkb) throws UDFArgumentException {
    return evaluate(wkb, 0);
  }

  public BytesWritable evaluate(BytesWritable wkb, int wkid) throws UDFArgumentException {

    try {
      SpatialReference spatialReference = null;
      if (wkid != GeometryUtils.WKID_UNKNOWN) {
        spatialReference = SpatialReference.create(wkid);
      }
      byte[] byteArr = wkb.getBytes();
      ByteBuffer byteBuf = ByteBuffer.allocate(byteArr.length);
      byteBuf.put(byteArr);
      OGCGeometry ogcObj = OGCGeometry.fromBinary(byteBuf);
      ogcObj.setSpatialReference(spatialReference);
      if (ogcObj.geometryType().equals("Polygon")) {
        return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
      } else {
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POLYGON, GeometryUtils.OGCType.UNKNOWN);
        return null;
      }
    } catch (Exception e) {  // IllegalArgumentException, GeometryException
      LOG.error(e.getMessage());
      return null;
    }
  }

}
