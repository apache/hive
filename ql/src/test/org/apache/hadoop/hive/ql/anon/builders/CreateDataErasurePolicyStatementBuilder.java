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

package org.apache.hadoop.hive.ql.anon.builders;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class CreateDataErasurePolicyStatementBuilder {

  private String policyName;
  private String policySource;
  private boolean ifNotExists;

  public CreateDataErasurePolicyStatementBuilder() {
  }

  public CreateDataErasurePolicyStatementBuilder(final String policyName, final String policySource) {
    this.policyName = policyName;
    this.policySource = policySource;
  }

  public CreateDataErasurePolicyStatementBuilder withPolicyName(final String policyName) {
    this.policyName = policyName;
    return this;
  }

  public CreateDataErasurePolicyStatementBuilder withPolicySource(final String policySource) {
    this.policySource = policySource;
    return this;
  }

  public CreateDataErasurePolicyStatementBuilder withIfNotExists() {
    this.ifNotExists = true;
    return this;
  }

  public String build() {
    try {
      final Path policyFile = Files.createTempFile(policyName + "_", ".erp");
      Files.write(policyFile, policySource.getBytes(StandardCharsets.UTF_8));
      return "LOAD ERASURE POLICY " + policyName + " FROM '" + policyFile + "'";
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

}
