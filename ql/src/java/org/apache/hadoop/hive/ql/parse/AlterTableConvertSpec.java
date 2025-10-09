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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.MoreObjects;

import java.util.Map;

public class AlterTableConvertSpec {

  public String getTargetType() {
    return targetType;
  }

  public Map<String, String> getTblProperties() {
    return tblProperties;
  }

  private final String targetType;
  private final Map<String, String> tblProperties;

  public AlterTableConvertSpec(String targetType, Map<String, String> tblProperties) {
    this.targetType = targetType;
    this.tblProperties = tblProperties;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("ConvertTo", targetType)
        .add("TBLProperties", tblProperties).toString();
  }
}
