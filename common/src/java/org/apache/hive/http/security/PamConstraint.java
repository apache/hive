/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.http.security;

import org.eclipse.jetty.security.Constraint;

/**
 * Utility class that creates a Jetty 12 Constraint for PAM authentication.
 */
public final class PamConstraint {
  private static final String PAM_ROLE = "pam";

  private PamConstraint() {
    // utility class
  }

  /**
   * Creates a Constraint configured for PAM authentication with the "pam" role.
   */
  public static Constraint create() {
    return Constraint.from("pam", Constraint.Authorization.SPECIFIC_ROLE, PAM_ROLE);
  }
}
