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

/**
 * One version of a named erasure policy. The policy header (just the name)
 * lives in {@link MErasurePolicy}; this class adds versioning + lifecycle audit.
 */
public class MErasurePolicyVersion {

  private MErasurePolicy policy;
  private String versionLabel;
  private String status;               // DRAFT | VALIDATED | ACTIVE | SUPERSEDED | INACTIVE
  private String identityFieldName;
  private String identityFieldType;    // INT | LONG | STRING
  private String schemaType;
  private String sourcePath;
  private String sourceChecksum;
  private String sourceText;           // raw .erp source of this version
  private String validatedBy;
  private Long   validatedTs;
  private String activatedBy;
  private Long   activatedTs;
  private String deactivatedBy;
  private Long   deactivatedTs;

  public MErasurePolicyVersion() {
  }

  public MErasurePolicyVersion(MErasurePolicy policy, String versionLabel, String status,
                               String identityFieldName, String identityFieldType,
                               String schemaType, String sourcePath, String sourceChecksum) {
    this.policy = policy;
    this.versionLabel = versionLabel;
    this.status = status;
    this.identityFieldName = identityFieldName;
    this.identityFieldType = identityFieldType;
    this.schemaType = schemaType;
    this.sourcePath = sourcePath;
    this.sourceChecksum = sourceChecksum;
  }

  public MErasurePolicy getPolicy() { return policy; }
  public void setPolicy(MErasurePolicy policy) { this.policy = policy; }

  public String getVersionLabel() { return versionLabel; }
  public void setVersionLabel(String versionLabel) { this.versionLabel = versionLabel; }

  public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }

  public String getIdentityFieldName() { return identityFieldName; }
  public void setIdentityFieldName(String identityFieldName) { this.identityFieldName = identityFieldName; }

  public String getIdentityFieldType() { return identityFieldType; }
  public void setIdentityFieldType(String identityFieldType) { this.identityFieldType = identityFieldType; }

  public String getSchemaType() { return schemaType; }
  public void setSchemaType(String schemaType) { this.schemaType = schemaType; }

  public String getSourcePath() { return sourcePath; }
  public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

  public String getSourceChecksum() { return sourceChecksum; }
  public void setSourceChecksum(String sourceChecksum) { this.sourceChecksum = sourceChecksum; }

  public String getSourceText() { return sourceText; }
  public void setSourceText(String sourceText) { this.sourceText = sourceText; }

  public String getValidatedBy() { return validatedBy; }
  public void setValidatedBy(String validatedBy) { this.validatedBy = validatedBy; }

  public Long getValidatedTs() { return validatedTs; }
  public void setValidatedTs(Long validatedTs) { this.validatedTs = validatedTs; }

  public String getActivatedBy() { return activatedBy; }
  public void setActivatedBy(String activatedBy) { this.activatedBy = activatedBy; }

  public Long getActivatedTs() { return activatedTs; }
  public void setActivatedTs(Long activatedTs) { this.activatedTs = activatedTs; }

  public String getDeactivatedBy() { return deactivatedBy; }
  public void setDeactivatedBy(String deactivatedBy) { this.deactivatedBy = deactivatedBy; }

  public Long getDeactivatedTs() { return deactivatedTs; }
  public void setDeactivatedTs(Long deactivatedTs) { this.deactivatedTs = deactivatedTs; }
}
