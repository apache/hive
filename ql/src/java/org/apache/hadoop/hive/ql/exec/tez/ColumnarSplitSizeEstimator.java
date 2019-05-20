/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.io.ColumnarSplit;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitSizeEstimator;

/**
 * Split size estimator for columnar file formats.
 */
public class ColumnarSplitSizeEstimator implements SplitSizeEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnarSplitSizeEstimator.class);

  @Override
  public long getEstimatedSize(InputSplit inputSplit) throws IOException {
    long colProjSize = inputSplit.getLength();

    if (inputSplit instanceof ColumnarSplit) {
      colProjSize = ((ColumnarSplit) inputSplit).getColumnarProjectionSize();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Estimated column projection size: " + colProjSize);
      }
    } else if (inputSplit instanceof HiveInputFormat.HiveInputSplit) {
      InputSplit innerSplit = ((HiveInputFormat.HiveInputSplit) inputSplit).getInputSplit();

      if (innerSplit instanceof ColumnarSplit) {
        colProjSize = ((ColumnarSplit) innerSplit).getColumnarProjectionSize();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Estimated column projection size: " + colProjSize);
        }
      }
    }
    if (colProjSize <= 0) {
      /* columnar splits of unknown size - estimate worst-case */
      return Integer.MAX_VALUE;
    }
    return colProjSize;
  }
}
