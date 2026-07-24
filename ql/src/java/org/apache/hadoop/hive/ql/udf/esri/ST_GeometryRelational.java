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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that all simple relational tests (contains, touches, ...) extend from
 */
public abstract class ST_GeometryRelational extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(ST_GeometryRelational.class);

  private static final int NUM_ARGS = 2;
  private static final int GEOM_1 = 0;
  private static final int GEOM_2 = 1;

  private transient HiveGeometryOIHelper geomHelper1;
  private transient HiveGeometryOIHelper geomHelper2;

  private transient boolean firstRun = true;
  private PreparedGeometry preparedGeom1 = null;

  /**
   * Execute the spatial relationship test between two geometries.
   */
  protected abstract boolean executeRelation(Geometry geom1, Geometry geom2);

  /**
   * Execute the spatial relationship test using a PreparedGeometry for the first argument.
   * Subclasses can override this for optimized prepared geometry operations.
   */
  protected boolean executeRelationPrepared(PreparedGeometry prepGeom1, Geometry geom2) {
    return executeRelation(prepGeom1.getGeometry(), geom2);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {

    if (OIs.length != NUM_ARGS) {
      throw new UDFArgumentException("Spatial relationship operators take exactly two arguments");
    }

    geomHelper1 = HiveGeometryOIHelper.create(OIs[GEOM_1], GEOM_1);
    geomHelper2 = HiveGeometryOIHelper.create(OIs[GEOM_2], GEOM_2);

    if (LOG.isDebugEnabled()) {
      LOG.debug("OI[0]=" + geomHelper1);
      LOG.debug("OI[1]=" + geomHelper2);
    }

    firstRun = true;
    preparedGeom1 = null;

    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {

    Geometry geom1 = geomHelper1.getGeometry(args);
    Geometry geom2 = geomHelper2.getGeometry(args);

    if (geom1 == null || geom2 == null) {
      return false;
    }

    if (firstRun && geomHelper1.isConstant()) {
      preparedGeom1 = PreparedGeometryFactory.prepare(geom1);
    }

    firstRun = false;

    if (preparedGeom1 != null) {
      return executeRelationPrepared(preparedGeom1, geom2);
    }
    return executeRelation(geom1, geom2);
  }

  @Override
  public void close() {
    GeometryUtils.cleanup();
  }
}
