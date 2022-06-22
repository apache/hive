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
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "ST_PointZ",
    value = "_FUNC_(x, y, z) - constructor for 3D point",
    extended = "Example:\n" + "SELECT _FUNC_(longitude, latitude, elevation) from src LIMIT 1;") public class ST_PointZ
    extends ST_Geometry {

  public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z) {
    return evaluate(x, y, z, null);
  }

  // ZM
  public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z, DoubleWritable m) {
    if (x == null || y == null || z == null) {
      return null;
    }
    Point stPt = new Point(x.get(), y.get(), z.get());
    if (m != null)
      stPt.setM(m.get());
    return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(stPt, null));
  }
}
