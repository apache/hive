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

@Description(name = "ST_MaxZ",
    value = "_FUNC_(geometry) - returns the maximum Z coordinate of geometry",
    extended = "Example:\n" + "  SELECT _FUNC_(ST_PointZ(1.5, 2.5, 2)) FROM src LIMIT 1;  -- 2\n"
        + "  SELECT _FUNC_(ST_LineString('linestring z (1.5 2.5 2, 3.0 2.2 1)')) FROM src LIMIT 1;  -- 1\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_MaxZ(ST_PointZ(0., 3., 1.)) from onerow",
//			result = "1.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxZ(ST_GeomFromText('linestring z (10 10 2, 20 20 4)')) from onerow",
//			result = "4.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxZ(ST_MultiPoint('multipoint z((0 0 1), (2 2 3))')) from onerow",
//			result = "3.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxZ(ST_Point(1,2)) from onerow",
//			result = "null"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxZ(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_MaxZ extends ST_GeometryAccessor {
  final DoubleWritable resultDouble = new DoubleWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_MaxZ.class.getName());

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
    if (!ogcGeometry.is3D()) {
      LogUtils.Log_Not3D(LOG);
      return null;
    }

    resultDouble.set(ogcGeometry.MaxZ());
    return resultDouble;
  }

}
