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

package org.apache.iceberg.mr.hive.partition;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CustomPartitionFunction;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Preconditions;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergBucket;

public class IcebergBucketPartitionFunction implements CustomPartitionFunction, Serializable {
  private static final long serialVersionUID = 1L;

  private final String columnName;
  private final int numBuckets;
  private transient GenericUDFIcebergBucket udf;
  private transient SettableDeferredObject[] deferredObjects;
  private transient ObjectInspector partitionValueInspector;

  public IcebergBucketPartitionFunction(String columnName, int numBuckets) {
    this.columnName = columnName;
    this.numBuckets = numBuckets;
  }

  @Override
  public List<String> getColumnNames() {
    return Collections.singletonList(columnName);
  }

  @Override
  public OptionalInt getNumBuckets() {
    return OptionalInt.of(numBuckets);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return partitionValueInspector;
  }

  @Override
  public Object getPartitionValue(Object[] partitionFields) {
    deferredObjects[0].set(partitionFields[0]);
    try {
      return udf.evaluate(deferredObjects);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void initialize(ObjectInspector[] objectInspectors) {
    Preconditions.checkArgument(objectInspectors.length == 1);
    udf = new GenericUDFIcebergBucket();
    deferredObjects = new SettableDeferredObject[1];
    deferredObjects[0] = new SettableDeferredObject();
    final ObjectInspector numBucketInspector = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, new IntWritable(numBuckets));
    final ObjectInspector[] udfInspectors = { objectInspectors[0], numBucketInspector };
    try {
      partitionValueInspector = udf.initialize(udfInspectors);
    } catch (UDFArgumentException e) {
      throw new IllegalArgumentException("The given object inspector is illegal", e);
    }
  }

  @Override
  public String toString() {
    return "Bucket{" +
        "columnName='" + columnName + '\'' +
        ", numBuckets=" + numBuckets +
        ", partitionValueInspector=" + partitionValueInspector +
        '}';
  }
}
