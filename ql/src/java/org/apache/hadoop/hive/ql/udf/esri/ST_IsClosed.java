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

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_IsClosed",
    value = "_FUNC_(ST_[Multi]LineString) - return true if the linestring or multi-line is closed",
    extended = "Example:\n" + "  SELECT _FUNC_(ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)) FROM src LIMIT 1;  -- true\n"
        + "  SELECT _FUNC_(ST_LineString(0.,0., 3.,4.)) FROM src LIMIT 1;  -- false\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(ST_LineString(0.,0., 3.,4.)) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(ST_MultiLineString('multilinestring ((0 0, 3 4, 2 2), (6 2, 7 8))')) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(ST_MultiLineString('multilinestring ((0 0, 3 4, 2 2, 0 0), (6 2, 7 8))')) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(ST_MultiLineString('multilinestring ((0 0, 3 4, 2 2, 0 0), (6 2, 7 5, 6 8, 6 2))')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsClosed(null) from onerow",
//			result = "null"
//			)
//		}
//	)

public class ST_IsClosed extends ST_GeometryAccessor {
  final BooleanWritable resultBoolean = new BooleanWritable();
  static final Logger LOG = LoggerFactory.getLogger(ST_IsClosed.class.getName());

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

      switch (GeometryUtils.getType(geomref)) {
      case ST_LINESTRING:
      case ST_MULTILINESTRING:
        MultiPath lines = (MultiPath) (ogcGeometry.getEsriGeometry());
        int nPaths = lines.getPathCount();
        boolean rslt = true;
        for (int ix = 0; rslt && ix < nPaths; ix++) {
          Point p0 = lines.getPoint(lines.getPathStart(ix));
          Point pf = lines.getPoint(lines.getPathEnd(ix) - 1);
          rslt = rslt && pf.equals(p0);  // no tolerance - OGC
        }
        resultBoolean.set(rslt);
        return resultBoolean;
      default:  // ST_IsClosed gives ERROR on Point or Polygon, on Postgres/Oracle
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_LINESTRING, GeometryUtils.getType(geomref));
        return null;
      }

    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_IsClosed" + e);
      return null;
    }

  }
}
