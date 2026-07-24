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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_PointFromWKB",
    value = "_FUNC_(wkb) - construct an ST_Point from OGC well-known binary",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_AsBinary(ST_GeomFromText('point (1 0))'))) FROM src LIMIT 1;  -- constructs ST_Point\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_PointFromWKB(ST_AsBinary(ST_GeomFromText('point (10 10)')))) from onerow",
//			result = "ST_POINT"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_PointFromWKB(ST_AsBinary(ST_GeomFromText('point (10 10)'))), ST_GeomFromText('point (10 10)')) from onerow",
//			result = "true"
//			)
//		}
//	)

public class ST_PointFromWKB extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_PointFromWKB.class.getName());

  public BytesWritable evaluate(BytesWritable wkb) throws UDFArgumentException {
    return evaluate(wkb, 0);
  }

  public BytesWritable evaluate(BytesWritable wkb, int wkid) throws UDFArgumentException {

    try {
      Geometry geom = GeometryUtils.wkbReader().read(wkb.getBytes());
      if (geom.getGeometryType().equals("Point")) {
        geom.setSRID(wkid);
        return GeometryUtils.geometryToEsriShapeBytesWritable(geom, wkid);
      } else {
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.OGCType.UNKNOWN);
        return null;
      }
    } catch (Exception e) {  // IllegalArgumentException, GeometryException
      LogUtils.Log_InternalError(LOG, "ST_PointFromWKB: " + e);
      return null;
    }
  }

}
