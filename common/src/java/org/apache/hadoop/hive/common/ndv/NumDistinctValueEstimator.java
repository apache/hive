/**
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
package org.apache.hadoop.hive.common.ndv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

public interface NumDistinctValueEstimator {

  static final Logger LOG = LoggerFactory.getLogger(NumDistinctValueEstimator.class.getName());

  public void reset();

  public String serialize();
  
  public NumDistinctValueEstimator deserialize(String s);

  public void addToEstimator(long v);

  public void addToEstimator(double d);

  public void addToEstimator(String s);

  public void addToEstimator(HiveDecimal decimal);

  public void mergeEstimators(NumDistinctValueEstimator o);

  public long estimateNumDistinctValues();

  public int lengthFor(JavaDataModel model);

  public boolean canMerge(NumDistinctValueEstimator o);

}
