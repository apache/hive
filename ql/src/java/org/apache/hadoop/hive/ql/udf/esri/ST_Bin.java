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

import com.esri.core.geometry.ogc.OGCPoint;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.EnumSet;

@Description(name = "ST_Bin",
    value = "_FUNC_(binsize, point) - return bin ID for given point\n")
public class ST_Bin extends GenericUDF {

  private transient HiveGeometryOIHelper geomHelper;
  private transient boolean binSizeIsConstant;
  private transient PrimitiveObjectInspector oiBinSize;
  private transient BinUtils bins;

  @Override
  public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {

    if (OIs.length != 2) {
      throw new UDFArgumentException("Function takes exactly 2 arguments");
    }

    if (OIs[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentException("Argument 0 must be a number - got: " + OIs[0].getCategory());
    }

    oiBinSize = (PrimitiveObjectInspector) OIs[0];
    if (!EnumSet.of(PrimitiveCategory.DECIMAL, PrimitiveCategory.DOUBLE, PrimitiveCategory.INT, PrimitiveCategory.LONG,
        PrimitiveCategory.SHORT, PrimitiveCategory.FLOAT).contains(oiBinSize.getPrimitiveCategory())) {
      throw new UDFArgumentException("Argument 0 must be a number - got: " + oiBinSize.getPrimitiveCategory());
    }

    geomHelper = HiveGeometryOIHelper.create(OIs[1], 1);
    binSizeIsConstant = ObjectInspectorUtils.isConstantObjectInspector(OIs[0]);

    return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    double binSize = PrimitiveObjectInspectorUtils.getDouble(args[0].get(), oiBinSize);

    if (!binSizeIsConstant || bins == null) {
      bins = new BinUtils(binSize);
    }

    OGCPoint point = geomHelper.getPoint(args);

    if (point == null) {
      return null;
    }

    return bins.getId(point.X(), point.Y());
  }

  @Override
  public String getDisplayString(String[] args) {
    assert (args.length == 2);
    return String.format("st_bin(%s,%s)", args[0], args[1]);
  }

}
