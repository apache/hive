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

package org.apache.iceberg.mr.hive.plan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CustomBucketFunction;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.SettableDeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergBucket;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergBucketFunction implements CustomBucketFunction {
  private static final long serialVersionUID = 1L;

  private final List<String> sourceColumnNames;
  private final List<Integer> numBuckets;

  private transient List<GenericUDFIcebergBucket> bucketUdfs;
  private transient List<IntObjectInspector> bucketIdInspectors;
  private transient SettableDeferredJavaObject[] deferredObjects;

  public IcebergBucketFunction(List<String> sourceColumnNames, List<Integer> numBuckets) {
    Objects.requireNonNull(sourceColumnNames);
    Objects.requireNonNull(numBuckets);
    Preconditions.checkArgument(sourceColumnNames.size() == numBuckets.size());
    Preconditions.checkArgument(!sourceColumnNames.isEmpty());
    this.sourceColumnNames = sourceColumnNames;
    this.numBuckets = numBuckets;
  }

  @Override
  public Optional<CustomBucketFunction> select(boolean[] retainedColumns) {
    Preconditions.checkArgument(retainedColumns.length == numBuckets.size());
    Preconditions.checkState(bucketUdfs == null);
    Preconditions.checkState(bucketIdInspectors == null);
    Preconditions.checkState(deferredObjects == null);

    final List<String> newSourceColumnNames = Lists.newArrayList();
    final List<Integer> newNumBuckets = Lists.newArrayList();
    for (int i = 0; i < retainedColumns.length; i++) {
      if (retainedColumns[i]) {
        newSourceColumnNames.add(sourceColumnNames.get(i));
        newNumBuckets.add(numBuckets.get(i));
      }
    }

    if (newSourceColumnNames.isEmpty()) {
      return Optional.empty();
    }

    final IcebergBucketFunction newBucketFunction = new IcebergBucketFunction(newSourceColumnNames, newNumBuckets);
    return Optional.of(newBucketFunction);
  }

  @Override
  public void initialize(ObjectInspector[] objectInspectors) {
    Preconditions.checkArgument(objectInspectors.length == numBuckets.size());
    bucketIdInspectors = Lists.newArrayList();
    bucketUdfs = Lists.newArrayList();
    for (int i = 0; i < objectInspectors.length; i++) {
      final GenericUDFIcebergBucket udf = new GenericUDFIcebergBucket();
      final ObjectInspector numBucketInspector = PrimitiveObjectInspectorFactory
          .getPrimitiveWritableConstantObjectInspector(
              TypeInfoFactory.intTypeInfo,
              new IntWritable(numBuckets.get(i)));
      final ObjectInspector[] args = { objectInspectors[i], numBucketInspector };
      try {
        final ObjectInspector inspector = udf.initialize(args);
        Preconditions.checkState(inspector.getCategory() == Category.PRIMITIVE);
        final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
        Preconditions.checkState(primitiveInspector.getPrimitiveCategory() == PrimitiveCategory.INT);
        bucketIdInspectors.add((IntObjectInspector) primitiveInspector);
      } catch (UDFArgumentException e) {
        throw new IllegalArgumentException("The given object inspector is illegal", e);
      }
      bucketUdfs.add(udf);
    }
    deferredObjects = new SettableDeferredJavaObject[1];
    deferredObjects[0] = new SettableDeferredJavaObject();

    Preconditions.checkState(bucketIdInspectors.size() == numBuckets.size());
    Preconditions.checkState(bucketUdfs.size() == numBuckets.size());
  }

  @Override
  public List<String> getSourceColumnNames() {
    return sourceColumnNames;
  }

  @Override
  public int getNumBuckets() {
    return numBuckets.stream().reduce(1, Math::multiplyExact);
  }

  @Override
  public int getBucketHashCode(Object[] bucketFields) {
    final int[] bucketIds = new int[numBuckets.size()];
    for (int i = 0; i < bucketUdfs.size(); i++) {
      deferredObjects[0].set(bucketFields[i]);
      try {
        final Object bucketId = bucketUdfs.get(i).evaluate(deferredObjects);
        bucketIds[i] = bucketIdInspectors.get(i).get(bucketId);
      } catch (HiveException e) {
        throw new IllegalArgumentException("Failed to evaluate the given objects", e);
      }
    }
    return getHashCode(bucketIds);
  }

  public static int getHashCode(int[] bucketIds) {
    int hashCode = 0;
    for (int bucketId : bucketIds) {
      hashCode = 31 * hashCode + bucketId;
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("IcebergBucketFunction{sourceColumnNames=%s,numBuckets=%s}",
        sourceColumnNames, numBuckets);
  }
}
