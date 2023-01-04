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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_SetSRID",
    value = "_FUNC_(<ST_Geometry>, SRID) - set the Spatial Reference ID of the geometry",
    extended = "Example:\n" + "  > SELECT _FUNC_(ST_SetSRID(ST_Point(1.5, 2.5), 4326)) FROM src LIMIT 1;\n"
        + "  -- create a point and then set its SRID to 4326")

public class ST_SetSRID extends ST_Geometry {
  static final Logger LOG = LoggerFactory.getLogger(ST_SetSRID.class.getName());

  public BytesWritable evaluate(BytesWritable geomref, IntWritable wkwrap) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    // just return the geometry ref without setting anything if wkid is null
    if (wkwrap == null) {
      return geomref;
    }

    int wkid = wkwrap.get();
    if (GeometryUtils.getWKID(geomref) != wkid) {
      GeometryUtils.setWKID(geomref, wkid);
    }

    return geomref;
  }
}
