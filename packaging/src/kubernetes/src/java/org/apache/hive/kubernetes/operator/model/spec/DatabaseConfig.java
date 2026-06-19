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

package org.apache.hive.kubernetes.operator.model.spec;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;

/** JDBC database connection configuration for the Hive Metastore backend. */
public record DatabaseConfig(
    @JsonPropertyDescription("Database type: derby, mysql, postgres, mssql, or oracle")
    @Default("derby")
    String type,
    @JsonPropertyDescription("JDBC connection URL")
    String url,
    @JsonPropertyDescription("JDBC driver class name")
    String driver,
    @JsonPropertyDescription("Database username")
    String username,
    @JsonPropertyDescription("Reference to a Kubernetes Secret containing the database password")
    SecretKeyRef passwordSecretRef,
    @JsonPropertyDescription(
        "URL to download the JDBC driver jar, e.g. "
        + "https://repo1.maven.org/maven2/org/postgresql/"
        + "postgresql/42.7.5/postgresql-42.7.5.jar")
    String driverJarUrl) {

  public DatabaseConfig {
    type = type != null ? type : "derby";
  }
}
