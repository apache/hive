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

@Description(name = "ST_Buffer",
    value = "_FUNC_(ST_Geometry, distance) - ST_Geometry buffered by distance",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_Point(0, 0), 1) FROM src LIMIT 1;   -- polygon approximating a unit circle\n") public class ST_Buffer
    extends ST_GeometryProcessing {

  static final Logger LOG = LoggerFactory.getLogger(ST_Buffer.class.getName());

  public BytesWritable evaluate(BytesWritable geometryref1, DoubleWritable distance) {
    if (geometryref1 == null || geometryref1.getLength() == 0 || distance == null) {
      return null;
    }

    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryref1);
    if (ogcGeometry == null) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    OGCGeometry bufferedGeometry = ogcGeometry.buffer(distance.get());
    // TODO persist type information (polygon vs multipolygon)
    return GeometryUtils.geometryToEsriShapeBytesWritable(bufferedGeometry);
  }
}
