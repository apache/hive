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
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_CoordDim",
    value = "_FUNC_(geometry) - return count of coordinate components",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 2\n"
        + "  > SELECT _FUNC_(ST_PointZ(1.5,2.5, 3) FROM src LIMIT 1;  -- 3\n"
        + "  > SELECT _FUNC_(ST_Point(1.5, 2.5, 3., 4.)) FROM src LIMIT 1;  -- 4\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_Point(0., 3.)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_PointZ(0., 3., 1)) from onerow",
//			result = "3"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_Point(0., 3., 1., 2.)) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_CoordDim extends ST_GeometryAccessor {
  final IntWritable resultInt = new IntWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_Is3D.class.getName());

  public IntWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
    if (ogcGeometry == null) {
      return null;
    }

    resultInt.set(ogcGeometry.coordinateDimension());
    return resultInt;
  }

}
