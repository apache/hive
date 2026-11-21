/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.util.ByteBuffers;

public class IcebergBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements BinaryObjectInspector, WriteObjectInspector {

  private static final IcebergBinaryObjectInspector INSTANCE =
      new IcebergBinaryObjectInspector(TypeInfoFactory.binaryTypeInfo);

  private static final Map<PrimitiveTypeInfo, IcebergBinaryObjectInspector> INSTANCES = new ConcurrentHashMap<>();

  public static IcebergBinaryObjectInspector get() {
    return INSTANCE;
  }

  /**
   * Returns an inspector for a Hive type whose values are binary, for example GEOMETRY.
   */
  public static IcebergBinaryObjectInspector get(PrimitiveTypeInfo typeInfo) {
    if (TypeInfoFactory.binaryTypeInfo.equals(typeInfo)) {
      return INSTANCE;
    }
    return INSTANCES.computeIfAbsent(typeInfo, IcebergBinaryObjectInspector::new);
  }

  private IcebergBinaryObjectInspector(PrimitiveTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    if (o instanceof GeospatialBound bound) {
      return toWkbPoint(bound);
    }
    return ByteBuffers.toByteArray((ByteBuffer) o);
  }

  /**
   * Metadata tables report the bounds of geospatial columns as {@link GeospatialBound} points rather than WKB.
   */
  private static byte[] toWkbPoint(GeospatialBound bound) {
    int dimensionCode = (bound.hasZ() ? 1 : 0) + (bound.hasM() ? 2 : 0);
    int dimensions = 2 + (bound.hasZ() ? 1 : 0) + (bound.hasM() ? 1 : 0);
    ByteBuffer wkb = ByteBuffer.allocate(1 + 4 + dimensions * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    wkb.put((byte) 1).putInt(1 + 1000 * dimensionCode).putDouble(bound.x()).putDouble(bound.y());
    if (bound.hasZ()) {
      wkb.putDouble(bound.z());
    }
    if (bound.hasM()) {
      wkb.putDouble(bound.m());
    }
    return wkb.array();
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new BytesWritable(getPrimitiveJavaObject(o));
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof byte[]) {
      byte[] bytes = (byte[]) o;
      return Arrays.copyOf(bytes, bytes.length);
    } else if (o instanceof ByteBuffer) {
      ByteBuffer copy =
          ByteBuffer.wrap(((ByteBuffer) o).array(), ((ByteBuffer) o).arrayOffset(), ((ByteBuffer) o).limit());
      return copy;
    } else {
      return o;
    }
  }

  @Override
  public ByteBuffer convert(Object o) {
    return o == null ? null : ByteBuffer.wrap((byte[]) o);
  }

}
