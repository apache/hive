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
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Description(name = "ST_GeomFromShape",
    value = "_FUNC_(shape) - construct ST_Geometry from Esri shape representation of geometry\n",
    extended = "Example:\n"
        + "  SELECT _FUNC_(ST_AsShape(ST_Point(1, 2))); -- constructs ST_Point\n") public class ST_GeomFromShape
    extends ST_Geometry {

  static final Logger LOG = LoggerFactory.getLogger(ST_GeomFromShape.class.getName());

  public BytesWritable evaluate(BytesWritable shape) throws UDFArgumentException {
    return evaluate(shape, 0);
  }

  public BytesWritable evaluate(BytesWritable shape, int wkid) throws UDFArgumentException {
    try {
      // The argument is a raw ESRI shape body (as produced by ST_AsShape), i.e. the legacy
      // ESRI shape encoding without the Hive transport header.
      ByteBuffer shapeBuffer = ByteBuffer.wrap(shape.getBytes(), 0, shape.getLength())
          .order(ByteOrder.LITTLE_ENDIAN);
      Geometry jtsGeom = EsriShapeConverter.fromEsriShapeBody(shapeBuffer);
      if (jtsGeom == null) {
        return null;
      }
      jtsGeom.setSRID(wkid);
      return GeometryUtils.geometryToEsriShapeBytesWritable(jtsGeom, wkid);
    } catch (Exception e) {
      LogUtils.Log_ExceptionThrown(LOG, "geom-from-shape", e);
      return null;
    }
  }

}
