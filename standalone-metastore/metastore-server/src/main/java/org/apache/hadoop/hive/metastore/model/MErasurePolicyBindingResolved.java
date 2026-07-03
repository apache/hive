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

package org.apache.hadoop.hive.metastore.model;

/** Materialised output of the conflict resolver at attach time. */
public class MErasurePolicyBindingResolved {

  private MErasurePolicyBinding binding;
  private String schemaValue;
  private String fieldPath;
  private String action;
  private String literalValue;
  private String literalType;
  private String params;
  private String contributingPolicies;   // JSON array of policy ids
  private String resolutionNote;

  public MErasurePolicyBindingResolved() {
  }

  public MErasurePolicyBindingResolved(MErasurePolicyBinding binding, String schemaValue,
                                       String fieldPath, String action, String literalValue,
                                       String literalType, String params,
                                       String contributingPolicies, String resolutionNote) {
    this.binding = binding;
    this.schemaValue = schemaValue;
    this.fieldPath = fieldPath;
    this.action = action;
    this.literalValue = literalValue;
    this.literalType = literalType;
    this.params = params;
    this.contributingPolicies = contributingPolicies;
    this.resolutionNote = resolutionNote;
  }

  public MErasurePolicyBinding getBinding() { return binding; }
  public void setBinding(MErasurePolicyBinding binding) { this.binding = binding; }

  public String getSchemaValue() { return schemaValue; }
  public void setSchemaValue(String schemaValue) { this.schemaValue = schemaValue; }

  public String getFieldPath() { return fieldPath; }
  public void setFieldPath(String fieldPath) { this.fieldPath = fieldPath; }

  public String getAction() { return action; }
  public void setAction(String action) { this.action = action; }

  public String getLiteralValue() { return literalValue; }
  public void setLiteralValue(String literalValue) { this.literalValue = literalValue; }

  public String getLiteralType() { return literalType; }
  public void setLiteralType(String literalType) { this.literalType = literalType; }

  public String getParams() { return params; }
  public void setParams(String params) { this.params = params; }

  public String getContributingPolicies() { return contributingPolicies; }
  public void setContributingPolicies(String contributingPolicies) { this.contributingPolicies = contributingPolicies; }

  public String getResolutionNote() { return resolutionNote; }
  public void setResolutionNote(String resolutionNote) { this.resolutionNote = resolutionNote; }
}
