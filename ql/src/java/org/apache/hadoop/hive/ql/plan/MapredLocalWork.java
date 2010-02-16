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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.BucketMatcher;

/**
 * MapredLocalWork.
 *
 */
@Explain(displayName = "Map Reduce Local Work")
public class MapredLocalWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork;
  private LinkedHashMap<String, FetchWork> aliasToFetchWork;
  private boolean inputFileChangeSensitive;
  private BucketMapJoinContext bucketMapjoinContext;
  
  public MapredLocalWork() {
  }

  public MapredLocalWork(
      final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork,
      final LinkedHashMap<String, FetchWork> aliasToFetchWork) {
    this.aliasToWork = aliasToWork;
    this.aliasToFetchWork = aliasToFetchWork;
  }

  @Explain(displayName = "Alias -> Map Local Operator Tree")
  public LinkedHashMap<String, Operator<? extends Serializable>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  /**
   * @return the aliasToFetchWork
   */
  @Explain(displayName = "Alias -> Map Local Tables")
  public LinkedHashMap<String, FetchWork> getAliasToFetchWork() {
    return aliasToFetchWork;
  }

  /**
   * @param aliasToFetchWork
   *          the aliasToFetchWork to set
   */
  public void setAliasToFetchWork(
      final LinkedHashMap<String, FetchWork> aliasToFetchWork) {
    this.aliasToFetchWork = aliasToFetchWork;
  }

  public boolean getInputFileChangeSensitive() {
    return inputFileChangeSensitive;
  }

  public void setInputFileChangeSensitive(boolean inputFileChangeSensitive) {
    this.inputFileChangeSensitive = inputFileChangeSensitive;
  }
  
  @Explain(displayName = "Bucket Mapjoin Context", normalExplain = false)
  public BucketMapJoinContext getBucketMapjoinContext() {
    return bucketMapjoinContext;
  }

  public void setBucketMapjoinContext(BucketMapJoinContext bucketMapjoinContext) {
    this.bucketMapjoinContext = bucketMapjoinContext;
  }
  
  public static class BucketMapJoinContext implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // used for bucket map join
    private LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasBucketFileNameMapping;
    private String mapJoinBigTableAlias;
    private Class<? extends BucketMatcher> bucketMatcherClass;

    public void setMapJoinBigTableAlias(String bigTableAlias) {
      this.mapJoinBigTableAlias = bigTableAlias;
    }

    public String getMapJoinBigTableAlias() {
      return mapJoinBigTableAlias;
    }

    public Class<? extends BucketMatcher> getBucketMatcherClass() {
      return bucketMatcherClass;
    }

    public void setBucketMatcherClass(
        Class<? extends BucketMatcher> bucketMatcherClass) {
      this.bucketMatcherClass = bucketMatcherClass;
    }

    @Explain(displayName = "Alias Bucket File Name Mapping", normalExplain = false)
    public LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> getAliasBucketFileNameMapping() {
      return aliasBucketFileNameMapping;
    }

    public void setAliasBucketFileNameMapping(
        LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasBucketFileNameMapping) {
      this.aliasBucketFileNameMapping = aliasBucketFileNameMapping;
    }
    
    public String toString() {
      if (aliasBucketFileNameMapping != null)
        return "Mapping:" + aliasBucketFileNameMapping.toString();
      else
        return "";
    }
  }
}
