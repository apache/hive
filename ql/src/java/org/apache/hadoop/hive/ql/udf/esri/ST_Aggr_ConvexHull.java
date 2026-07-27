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
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@Description(name = "ST_Aggr_ConvexHull",
    value = "_FUNC_(ST_Geometry) - aggregate convex hull of all geometries passed",
    extended = "Example:\n"
        + "  SELECT _FUNC_(geometry) FROM source; -- return convex hull of all geometries in source")

public class ST_Aggr_ConvexHull extends UDAF {
  static final Logger LOG = LoggerFactory.getLogger(ST_Aggr_ConvexHull.class.getName());

  public static class AggrConvexHullBinaryEvaluator implements UDAFEvaluator {

    private final ArrayList<Geometry> geometries = new ArrayList<>();
    int firstWKID = -2;

    /*
     * Initialize evaluator
     */
    @Override
    public void init() {
      geometries.clear();
      firstWKID = -2;
    }

    /*
     * Iterate is called once per row in a table
     */
    public boolean iterate(BytesWritable geomref) throws HiveException {

      if (geomref == null) {
        LogUtils.Log_ArgumentsNull(LOG);
        return false;
      }

      if (firstWKID == -2) {
        firstWKID = GeometryUtils.getWKID(geomref);
      } else if (firstWKID != GeometryUtils.getWKID(geomref)) {
        LogUtils.Log_SRIDMismatch(LOG, geomref, firstWKID);
        return false;
      }

      Geometry geom = GeometryUtils.geometryFromEsriShape(geomref);
      if (geom != null) {
        geometries.add(geom);
      }

      return !geometries.isEmpty();
    }

    /*
     * Merge the current state of this evaluator with the result of another evaluator's terminatePartial()
     */
    public boolean merge(BytesWritable other) throws HiveException {
      // for our purposes, merge is the same as iterate
      return iterate(other);
    }

    public BytesWritable terminatePartial() throws HiveException {
      if (geometries.isEmpty()) {
        return null;
      }
      try {
        GeometryCollection collection = GeometryUtils.GEOMETRY_FACTORY
            .createGeometryCollection(geometries.toArray(new Geometry[0]));
        Geometry result = collection.convexHull();
        int wkid = (firstWKID == -2) ? GeometryUtils.WKID_UNKNOWN : firstWKID;
        return GeometryUtils.geometryToEsriShapeBytesWritable(result, wkid);
      } catch (Exception e) {
        LogUtils.Log_InternalError(LOG, "ST_Aggr_ConvexHull: " + e);
      } finally {
        geometries.clear();
        firstWKID = -2;
      }
      return null;
    }

    /*
     * Return a geometry that is the aggregation of all geometries added up until this point
     */
    public BytesWritable terminate() throws HiveException {
      // for our purposes, terminate is the same as terminatePartial
      return terminatePartial();
    }

  }
}
