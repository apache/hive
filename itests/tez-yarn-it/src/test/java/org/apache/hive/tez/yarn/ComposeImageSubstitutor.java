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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.tez.yarn;

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

/** Replaces Testcontainers' pinned docker:24.0.2 compose helper with a newer CLI for modern daemons.
 *  Registered via testcontainers.properties; override with -Dtez.yarn.compose.image=docker:tag. */
public class ComposeImageSubstitutor extends ImageNameSubstitutor {

  private static final String REPLACEMENT_IMAGE =
      System.getProperty("tez.yarn.compose.image", "docker:27.5.1");

  @Override
  public DockerImageName apply(DockerImageName original) {
    String unversioned = original.getUnversionedPart();
    if ("docker".equals(unversioned) || unversioned.endsWith("/docker")) {
      return DockerImageName.parse(REPLACEMENT_IMAGE);
    }
    return original;
  }

  @Override
  protected String getDescription() {
    return "tez-yarn-it compose image substitutor (docker -> " + REPLACEMENT_IMAGE + ")";
  }
}
