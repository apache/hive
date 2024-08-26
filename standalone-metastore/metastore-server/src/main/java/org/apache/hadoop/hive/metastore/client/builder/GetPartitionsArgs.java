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
package org.apache.hadoop.hive.metastore.client.builder;

import java.util.List;

public class GetPartitionsArgs {
  private String filter;
  private byte[] expr;
  private String defaultPartName;
  private int max;
  private List<String> partNames;
  private List<String> part_vals;
  private String userName;
  private List<String> groupNames;
  private String includeParamKeyPattern;
  private String excludeParamKeyPattern;
  private boolean skipColumnSchemaForPartition;

  private GetPartitionsArgs() {

  }

  public String getFilter() {
    return filter;
  }

  public byte[] getExpr() {
    return expr;
  }

  public String getDefaultPartName() {
    return defaultPartName;
  }

  public int getMax() {
    return max;
  }

  public List<String> getPartNames() {
    return partNames;
  }

  public List<String> getPart_vals() {
    return part_vals;
  }

  public String getUserName() {
    return userName;
  }

  public List<String> getGroupNames() {
    return groupNames;
  }

  public String getIncludeParamKeyPattern() {
    return includeParamKeyPattern;
  }

  public String getExcludeParamKeyPattern() {
    return excludeParamKeyPattern;
  }

  public boolean isSkipColumnSchemaForPartition() {
    return skipColumnSchemaForPartition;
  }

  public static class GetPartitionsArgsBuilder {
    private String filter;
    private byte[] expr;
    private String defaultPartName;
    private int max = -1;
    private List<String> partNames;
    private List<String> part_vals;
    private String userName;
    private List<String> groupNames;
    private String includeParamKeyPattern;
    private String excludeParamKeyPattern;
    private boolean skipColumnSchemaForPartition = false;

    public GetPartitionsArgsBuilder() {

    }

    public GetPartitionsArgsBuilder(GetPartitionsArgs args) {
      this.filter = args.filter;
      this.expr = args.expr;
      this.defaultPartName = args.defaultPartName;
      this.max = args.max;
      this.partNames = args.partNames;
      this.part_vals = args.part_vals;
      this.userName = args.userName;
      this.groupNames = args.groupNames;
      this.includeParamKeyPattern = args.includeParamKeyPattern;
      this.excludeParamKeyPattern = args.excludeParamKeyPattern;
      this.skipColumnSchemaForPartition = args.skipColumnSchemaForPartition;
    }

    public GetPartitionsArgsBuilder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public GetPartitionsArgsBuilder expr(byte[] expr) {
      this.expr = expr;
      return this;
    }

    public GetPartitionsArgsBuilder defaultPartName(String defaultPartName) {
      this.defaultPartName = defaultPartName;
      return this;
    }

    public GetPartitionsArgsBuilder max(int max) {
      this.max = max;
      return this;
    }

    public GetPartitionsArgsBuilder partNames(List<String> partNames) {
      this.partNames = partNames;
      return this;
    }

    public GetPartitionsArgsBuilder part_vals(List<String> part_vals) {
      this.part_vals = part_vals;
      return this;
    }

    public GetPartitionsArgsBuilder userName(String userName) {
      this.userName = userName;
      return this;
    }

    public GetPartitionsArgsBuilder groupNames(List<String> groupNames) {
      this.groupNames = groupNames;
      return this;
    }

    public GetPartitionsArgsBuilder includeParamKeyPattern(String includeParamKeyPattern) {
      this.includeParamKeyPattern = includeParamKeyPattern;
      return this;
    }

    public GetPartitionsArgsBuilder excludeParamKeyPattern(String excludeParamKeyPattern) {
      this.excludeParamKeyPattern = excludeParamKeyPattern;
      return this;
    }

    public GetPartitionsArgsBuilder skipColumnSchemaForPartition(boolean skipColumnSchemaForPartition) {
      this.skipColumnSchemaForPartition = skipColumnSchemaForPartition;
      return this;
    }

    public GetPartitionsArgs build() {
      GetPartitionsArgs additionalArgs = new GetPartitionsArgs();
      additionalArgs.filter = filter;
      additionalArgs.expr = expr;
      additionalArgs.defaultPartName = defaultPartName;
      additionalArgs.max = max;
      additionalArgs.partNames = partNames;
      additionalArgs.part_vals = part_vals;
      additionalArgs.userName = userName;
      additionalArgs.groupNames = groupNames;
      additionalArgs.includeParamKeyPattern = includeParamKeyPattern;
      additionalArgs.excludeParamKeyPattern = excludeParamKeyPattern;
      additionalArgs.skipColumnSchemaForPartition = skipColumnSchemaForPartition;
      return additionalArgs;
    }
  }
}
