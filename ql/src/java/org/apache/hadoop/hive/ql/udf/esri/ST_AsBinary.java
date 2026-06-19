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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

@Description(name = "ST_AsBinary",
    value = "_FUNC_(ST_Geometry) - return Well-Known Binary (WKB) representation of geometry\n",
    extended = "Example:\n" + "  SELECT _FUNC_(ST_Point(1, 2)) FROM onerow; -- WKB representation of POINT (1 2)\n")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('linestring (10 40, 40 30)')))) from onerow",
//			result = "ST_LINESTRING"
//			)
//		}
//	)

public class ST_AsBinary extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_AsBinary.class.getName());

  public BytesWritable evaluate(BytesWritable geomref) {
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
      ByteBuffer byteBuf = ogcGeometry.asBinary();
      byte[] byteArr = byteBuf.array();
      return new BytesWritable(byteArr);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }
  }
}
