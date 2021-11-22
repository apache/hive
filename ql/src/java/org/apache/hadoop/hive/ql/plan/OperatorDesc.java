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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface OperatorDesc extends Serializable, Cloneable {
  public Object clone() throws CloneNotSupportedException;
  public Statistics getStatistics();
  public void setStatistics(Statistics statistics);
  public OpTraits getTraits();
  public void setTraits(OpTraits opTraits);
  public Map<String, String> getOpProps();
  public long getMemoryNeeded();
  public void setMemoryNeeded(long memoryNeeded);
  public long getMaxMemoryAvailable();
  public void setMaxMemoryAvailable(long memoryAvailable);
  public String getRuntimeStatsTmpDir();
  public void setRuntimeStatsTmpDir(String runtimeStatsTmpDir);

  boolean isSame(OperatorDesc other);
  public Map<String, ExprNodeDesc> getColumnExprMap();
  public void setColumnExprMap(Map<String, ExprNodeDesc> colExprMap);

  void fillSignature(Map<String, Object> ret);

  public void setBucketingVersion(int bucketingVersion);

  public int getBucketingVersion();

  void addComputedField(String column);

  Set<String> getComputedFields();
}
