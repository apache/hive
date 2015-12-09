/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolverImpl;
import org.apache.hive.hcatalog.streaming.mutate.worker.Mutator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorImpl;
import org.apache.hive.hcatalog.streaming.mutate.worker.RecordInspector;
import org.apache.hive.hcatalog.streaming.mutate.worker.RecordInspectorImpl;

public class ReflectiveMutatorFactory implements MutatorFactory {

  private final int recordIdColumn;
  private final ObjectInspector objectInspector;
  private final Configuration configuration;
  private final int[] bucketColumnIndexes;

  public ReflectiveMutatorFactory(Configuration configuration, Class<?> recordClass, int recordIdColumn,
      int[] bucketColumnIndexes) {
    this.configuration = configuration;
    this.recordIdColumn = recordIdColumn;
    this.bucketColumnIndexes = bucketColumnIndexes;
    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(recordClass,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
  }

  @Override
  public Mutator newMutator(AcidOutputFormat<?, ?> outputFormat, long transactionId, Path partitionPath, int bucketId)
    throws IOException {
    return new MutatorImpl(configuration, recordIdColumn, objectInspector, outputFormat, transactionId, partitionPath,
        bucketId);
  }

  @Override
  public RecordInspector newRecordInspector() {
    return new RecordInspectorImpl(objectInspector, recordIdColumn);
  }

  @Override
  public BucketIdResolver newBucketIdResolver(int totalBuckets) {
    return new BucketIdResolverImpl(objectInspector, recordIdColumn, totalBuckets, bucketColumnIndexes);
  }

}
