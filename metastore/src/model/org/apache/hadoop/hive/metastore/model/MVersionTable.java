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

package org.apache.hadoop.hive.metastore.model;

public class MVersionTable {
  private String schemaVersion;
  private String versionComment;
  private String schemaVersionV2;

  public MVersionTable() {}

  public MVersionTable(String schemaVersion, String versionComment, String schemaVersionV2) {
    this.schemaVersion = schemaVersion;
    this.versionComment = versionComment;
    this.schemaVersionV2 = schemaVersionV2;
  }
  /**
   * @return the versionComment
   */
  public String getVersionComment() {
    return versionComment;
  }
  /**
   * @param versionComment the versionComment to set
   */
  public void setVersionComment(String versionComment) {
    this.versionComment = versionComment;
  }

  /**
   * @return the schemaVersion
   */
  public String getSchemaVersion() {
    return schemaVersion;
  }
  /**
   * @param schemaVersion the schemaVersion to set
   */
  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public String getSchemaVersionV2() {
    return schemaVersionV2;
  }

  public void setSchemaVersionV2(String schemaVersionV2) {
    this.schemaVersionV2 = schemaVersionV2;
  }

}
