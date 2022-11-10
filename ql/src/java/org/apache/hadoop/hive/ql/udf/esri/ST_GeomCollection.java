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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_GeomCollection",
    value = "_FUNC_(wkt) - construct a multi-part ST_Geometry from OGC well-known text",
    extended = "Example:\n"
        + "  > SELECT _FUNC_('multipoint ((1 0), (2 3))') FROM src LIMIT 1;  -- constructs ST_MultiPoint\n"
        + "OGC Compliance Notes : \n"
        + " ST_GeomCollection on Hive does not support collections - only multi-part geometries.\n"
        + "ST_GeomCollection('POINT(1 1), LINESTRING(2 0,3 0)') -- not supported\n")

//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_GeomCollection('MULTIPOINT ((10 40), (40 30))'), ST_GeomFromText('MULTIPOINT ((10 40), (40 30))')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_GeomCollection('multilinestring ((2 4, 10 10), (20 20, 7 8))'), ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_GeomCollection('multipolygon (((3 3, 4 6, 5 3, 3 3)),((8 24, 9 25, 1 28, 8 24)))'), ST_GeomFromText('multipolygon (((3 3, 4 6, 5 3, 3 3)),((8 24, 9 25, 1 28, 8 24)))')) from onerow",
//			result = "true"
//			)
//		}
//	)

public class ST_GeomCollection extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_GeomCollection.class.getName());

  public BytesWritable evaluate(Text wkt) throws UDFArgumentException {
    return evaluate(wkt, 0);
  }

  public BytesWritable evaluate(Text wkwrap, int wkid) throws UDFArgumentException {

    String wkt = wkwrap.toString();

    try {
      Geometry geomObj = GeometryEngine.geometryFromWkt(wkt, 0, Geometry.Type.Unknown);
      SpatialReference spatialReference = null;  // Idea: OGCGeometry.setSpatialReference after .fromText
      if (wkid != GeometryUtils.WKID_UNKNOWN) {
        spatialReference = SpatialReference.create(wkid);
      }
      OGCGeometry ogcObj = OGCGeometry.createFromEsriGeometry(geomObj, spatialReference);
      return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
    } catch (Exception e) {  // IllegalArgumentException, GeometryException
      LogUtils.Log_InvalidText(LOG, wkt);
      return null;
    }
  }

}
