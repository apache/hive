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

import org.apache.hive.common.util.SuppressFBWarnings;
import org.eclipse.jetty.util.security.Constraint;

public class PamConstraint extends Constraint {
  private static final String[] roles = { "pam" };

  @Override
  public boolean getAuthenticate() {
    return true;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Ref external obj for efficiency")
  public String[] getRoles() {
    return roles;
  }
}

