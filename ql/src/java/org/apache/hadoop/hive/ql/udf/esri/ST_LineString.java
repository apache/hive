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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
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
      Polyline linestring = new Polyline();
      linestring.startPath(xyPairs[0].get(), xyPairs[1].get());

      for (int i = 2; i < xyPairs.length; i += 2) {
        linestring.lineTo(xyPairs[i].get(), xyPairs[i + 1].get());
      }

      return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(linestring, null));
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
      Polyline linestring = new Polyline();

      for (int ix = 0; ix < xs.size(); ++ix) {
        DoubleWritable xdw = xs.get(ix), ydw = ys.get(ix);
        if (xdw == null || ydw == null) {
          LogUtils.Log_ArgumentsNull(LOG);
        }
        if (ix == 0) {
          linestring.startPath(xdw.get(), ydw.get());
        } else {
          linestring.lineTo(xdw.get(), ydw.get());
        }
      }

      return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(linestring, null));
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
      Polyline linestring = new Polyline();

      for (int ix = 0; ix < points.size(); ++ix) {
        BytesWritable geomref = points.get(ix);
        OGCGeometry gcur = GeometryUtils.geometryFromEsriShape(geomref);
        if (gcur == null || GeometryUtils.getType(geomref) != GeometryUtils.OGCType.ST_POINT) {
          if (gcur == null)
            LogUtils.Log_ArgumentsNull(LOG);
          else
            LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.getType(geomref));
          return null;
        }
        if (ix == 0) {
          linestring.startPath((Point) gcur.getEsriGeometry());
        } else {
          linestring.lineTo((Point) gcur.getEsriGeometry());
        }
      }

      return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(linestring, null));
    } catch (Exception e) {
      LogUtils.Log_InternalError(LOG, "ST_LineString: " + e);
      return null;
    }
  }

  // WKT constructor - can use SetSRID on constructed multi-point
  public BytesWritable evaluate(Text wkwrap) throws UDFArgumentException {
    String wkt = wkwrap.toString();
    try {
      OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
      ogcObj.setSpatialReference(null);
      if (ogcObj.geometryType().equals("LineString")) {
        return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
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
