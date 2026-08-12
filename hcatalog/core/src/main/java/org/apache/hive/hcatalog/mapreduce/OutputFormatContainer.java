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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 *  This container class is used to wrap OutputFormat implementations and augment them with
 *  behavior necessary to work with HCatalog (ie metastore updates, hcatalog delegation tokens, etc).
 *  Containers are also used to provide storage specific implementations of some HCatalog features (ie dynamic partitioning).
 *  Hence users wishing to create storage specific implementations of HCatalog features should implement this class and override
 *  HCatStorageHandler.getOutputFormatContainer(OutputFormat outputFormat) to return the implementation.
 *  By default DefaultOutputFormatContainer is used, which only implements the bare minimum features HCatalog features
 *  such as partitioning isn't supported.
 */
abstract class OutputFormatContainer extends OutputFormat<WritableComparable<?>, HCatRecord> {
  private org.apache.hadoop.mapred.OutputFormat<? super WritableComparable<?>, ? super Writable> of;

  /**
   * @param of OutputFormat this instance will contain
   */
  public OutputFormatContainer(org.apache.hadoop.mapred.OutputFormat<? super WritableComparable<?>, ? super Writable> of) {
    this.of = of;
  }

  /**
   * @return underlying OutputFormat
   */
  public org.apache.hadoop.mapred.OutputFormat getBaseOutputFormat() {
    return of;
  }

}
