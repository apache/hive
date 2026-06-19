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

@Description(name = "ST_SRID",
    value = "_FUNC_(ST_Geometry) - get the Spatial Reference ID of the geometry",
    extended = "Example:\n" + "  SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1  -- returns SRID 0")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_SRID(ST_SetSRID(ST_Point(1.1, 2.2), 4326)) FROM onerow",
//			result = "4326"
//		)
//	}
//)

public class ST_SRID extends ST_GeometryAccessor {
  static final Logger LOG = LoggerFactory.getLogger(ST_SRID.class.getName());

  IntWritable resultInt = new IntWritable();

  public IntWritable evaluate(BytesWritable geomref) {
    if (geomref == null || geomref.getLength() == 0) {
      LogUtils.Log_ArgumentsNull(LOG);
      return null;
    }

    resultInt.set(GeometryUtils.getWKID(geomref));
    return resultInt;
  }
}
