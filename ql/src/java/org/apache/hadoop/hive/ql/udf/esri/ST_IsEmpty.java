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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_IsEmpty",
    value = "_FUNC_(geometry) - return true if the geometry object is empty of geometric information",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- false\n"
        + "  > SELECT _FUNC_(ST_GeomFromText('point empty')) FROM src LIMIT 1;  -- true\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_GeomFromText('point empty')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_Intersection(st_point(2,0), ST_Point(1,1))) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_GeomFromText('point (10.02 20.01)')) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(null) from onerow",
//			result = "null"
//			)
//		}
//	)

public class ST_IsEmpty extends ST_GeometryAccessor {
  final BooleanWritable resultBoolean = new BooleanWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_IsEmpty.class.getName());

  public BooleanWritable evaluate(BytesWritable geomref) {
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
      resultBoolean.set(ogcGeometry.isEmpty());
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_IsEmpty" + e);
      return null;
    }
    return resultBoolean;
  }
}
