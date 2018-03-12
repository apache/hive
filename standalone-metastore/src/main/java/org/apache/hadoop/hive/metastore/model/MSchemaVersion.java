/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.model;

public class MSchemaVersion {
  private MISchema iSchema;
  private int version;
  private long createdAt;
  private MColumnDescriptor cols;
  private int state;
  private String description;
  private String schemaText;
  private String fingerprint;
  private String name;
  private MSerDeInfo serDe;

  public MSchemaVersion(MISchema iSchema, int version, long createdAt,
                        MColumnDescriptor cols, int state, String description,
                        String schemaText, String fingerprint, String name,
                        MSerDeInfo serDe) {
    this.iSchema = iSchema;
    this.version = version;
    this.createdAt = createdAt;
    this.cols = cols;
    this.state = state;
    this.description = description;
    this.schemaText = schemaText;
    this.fingerprint = fingerprint;
    this.name = name;
    this.serDe = serDe;
  }

  public MISchema getiSchema() {
    return iSchema;
  }

  public void setiSchema(MISchema iSchema) {
    this.iSchema = iSchema;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public MColumnDescriptor getCols() {
    return cols;
  }

  public void setCols(MColumnDescriptor cols) {
    this.cols = cols;
  }

  public int getState() {
    return state;
  }

  public void setState(int state) {
    this.state = state;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getSchemaText() {
    return schemaText;
  }

  public void setSchemaText(String schemaText) {
    this.schemaText = schemaText;
  }

  public String getFingerprint() {
    return fingerprint;
  }

  public void setFingerprint(String fingerprint) {
    this.fingerprint = fingerprint;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public MSerDeInfo getSerDe() {
    return serDe;
  }

  public void setSerDe(MSerDeInfo serDe) {
    this.serDe = serDe;
  }
}
