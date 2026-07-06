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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@Description(name = "ST_LineString",
    value = "_FUNC_(x, y, [x, y]*) - constructor for 2D line string\n"
        + "_FUNC_(array(x+), array(y+)) - constructor for 2D line string\n"
        + "_FUNC_(array(ST_Point(x,y)+)) - constructor for 2D line string\n"
        + "_FUNC_('linestring( ... )') - constructor for 2D line string",
    extended = "Example:\n" + "  SELECT _FUNC_(1, 1, 2, 2, 3, 3) from src LIMIT 1;\n"
        + "  SELECT _FUNC_('linestring(1 1, 2 2, 3 3)') from src LIMIT 1;")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_Linestring('linestring (10 10, 20 20)')) from onerow",
//			result = "ST_LINESTRING"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_Linestring('linestring (10 10, 20 20)'), ST_GeomFromText('linestring (10 10, 20 20)')) from onerow",
//			result = "true"
//			)
//		}
//	)

public class ST_LineString extends ST_Geometry {
  static final Logger LOG = LoggerFactory.getLogger(ST_LineString.class.getName());

  // Number-pairs constructor
  public BytesWritable evaluate(DoubleWritable... xyPairs) throws UDFArgumentException {

    if (xyPairs == null || xyPairs.length == 0 || xyPairs.length % 2 != 0) {
      return null;
    }

    try {
      Coordinate[] coords = new Coordinate[xyPairs.length / 2];
      for (int i = 0; i < xyPairs.length; i += 2) {
        coords[i / 2] = new Coordinate(xyPairs[i].get(), xyPairs[i + 1].get());
      }
      Geometry linestring = GeometryUtils.GEOMETRY_FACTORY.createLineString(coords);
      return GeometryUtils.geometryToEsriShapeBytesWritable(linestring);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_LineString: " + e);
      return null;
    }
  }

  // constructor from arrays of X and Y coordinates
  public BytesWritable evaluate(ArrayList<DoubleWritable> xs, ArrayList<DoubleWritable> ys)
      throws UDFArgumentException {
    if (null == xs || null == ys || xs.size() == 0 || ys.size() == 0 || xs.size() != ys.size()) {
      return null;
    }

    try {
      Coordinate[] coords = new Coordinate[xs.size()];
      for (int ix = 0; ix < xs.size(); ++ix) {
        DoubleWritable xdw = xs.get(ix), ydw = ys.get(ix);
        if (xdw == null || ydw == null) {
          LogUtils.Log_ArgumentsNull(LOG);
        }
        coords[ix] = new Coordinate(xdw.get(), ydw.get());
      }
      Geometry linestring = GeometryUtils.GEOMETRY_FACTORY.createLineString(coords);
      return GeometryUtils.geometryToEsriShapeBytesWritable(linestring);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_LineString: " + e);
      return null;
    }
  }

  // constructor from array of points
  public BytesWritable evaluate(ArrayList<BytesWritable> points) throws UDFArgumentException {
    if (null == points || points.size() == 0) {
      return null;
    }

    try {
      Coordinate[] coords = new Coordinate[points.size()];
      for (int ix = 0; ix < points.size(); ++ix) {
        BytesWritable geomref = points.get(ix);
        Geometry gcur = GeometryUtils.geometryFromEsriShape(geomref);
        if (gcur == null || GeometryUtils.getType(geomref) != GeometryUtils.OGCType.ST_POINT) {
          if (gcur == null)
            LogUtils.Log_ArgumentsNull(LOG);
          else
            LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.getType(geomref));
          return null;
        }
        Point pt = (Point) gcur;
        coords[ix] = pt.getCoordinate();
      }
      Geometry linestring = GeometryUtils.GEOMETRY_FACTORY.createLineString(coords);
      return GeometryUtils.geometryToEsriShapeBytesWritable(linestring);
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_LineString: " + e);
      return null;
    }
  }

  // WKT constructor - can use SetSRID on constructed multi-point
  public BytesWritable evaluate(Text wkwrap) throws UDFArgumentException {
    String wkt = wkwrap.toString();
    try {
      Geometry geom = GeometryUtils.wktReader().read(wkt);
      if (geom.getGeometryType().equals("LineString")) {
        return GeometryUtils.geometryToEsriShapeBytesWritable(geom);
      } else {
        LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_LINESTRING, GeometryUtils.OGCType.UNKNOWN);
        return null;
      }

    } catch (Exception e) {  // IllegalArgumentException, GeometryException
      LogUtils.Log_InvalidText(LOG, wkt);
      return null;
    }
  }

}
