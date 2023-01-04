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

package org.apache.hive.service.auth;

import org.apache.commons.lang3.EnumUtils;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * AuthType is used to parse and verify
 * {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#HIVE_SERVER2_AUTHENTICATION}.
 * Throws an exception if the config value is not allowed.
 */
public class AuthType {
  static final Set<HiveAuthConstants.AuthTypes> PASSWORD_BASED_TYPES = new HashSet<>(Arrays.asList(
      HiveAuthConstants.AuthTypes.LDAP, HiveAuthConstants.AuthTypes.CUSTOM, HiveAuthConstants.AuthTypes.PAM));
  private final BitSet typeBits;

  public AuthType(String authTypes) throws Exception {
    typeBits = new BitSet();
    parseTypes(authTypes);
    verifyTypes(authTypes);
  }

  private void parseTypes(String authTypes) throws Exception {
    String[] types = authTypes.split(",");
    for (String type : types) {
      if (!EnumUtils.isValidEnumIgnoreCase(HiveAuthConstants.AuthTypes.class, type)) {
        throw new Exception(type + " is not a valid authentication type.");
      }
      typeBits.set(EnumUtils.getEnumIgnoreCase(HiveAuthConstants.AuthTypes.class, type).ordinal());
    }
  }

  private void verifyTypes(String authTypes) throws Exception {
    if (typeBits.cardinality() == 1) {
      // single authentication type has no conflicts
      return;
    }
    if (typeBits.get(HiveAuthConstants.AuthTypes.SAML.ordinal()) &&
        !typeBits.get(HiveAuthConstants.AuthTypes.NOSASL.ordinal()) &&
        !typeBits.get(HiveAuthConstants.AuthTypes.KERBEROS.ordinal()) &&
        !typeBits.get(HiveAuthConstants.AuthTypes.NONE.ordinal()) &&
        (!areAnyEnabled(PASSWORD_BASED_TYPES) || isExactlyOneEnabled(PASSWORD_BASED_TYPES))) {
      // SAML can be enabled with another password based authentication types
      return;
    }
    throw new Exception("The authentication types have conflicts: " + authTypes);
  }

  private boolean isExactlyOneEnabled(Collection<HiveAuthConstants.AuthTypes> types) {
    boolean areAnyEnabled = false;
    boolean areTwoEnabled = false;
    Iterator<HiveAuthConstants.AuthTypes> it = types.iterator();
    while (!areTwoEnabled && it.hasNext()) {
      boolean isCurrentTypeEnabled = isEnabled(it.next());
      areTwoEnabled = areAnyEnabled && isCurrentTypeEnabled;
      areAnyEnabled |= isCurrentTypeEnabled;
    }
    return areAnyEnabled && !areTwoEnabled;
  }

  private boolean areAnyEnabled(Collection<HiveAuthConstants.AuthTypes> types) {
    boolean areAnyEnabled = false;
    Iterator<HiveAuthConstants.AuthTypes> it = types.iterator();
    while (!areAnyEnabled && it.hasNext()) {
      areAnyEnabled = isEnabled(it.next());
    }
    return areAnyEnabled;
  }

  public boolean isEnabled(HiveAuthConstants.AuthTypes type) {
    return typeBits.get(type.ordinal());
  }


  public String getPasswordBasedAuthStr() {
    if (isEnabled(HiveAuthConstants.AuthTypes.NOSASL)) {
      return HiveAuthConstants.AuthTypes.NOSASL.getAuthName();
    }
    if (isEnabled(HiveAuthConstants.AuthTypes.NONE)) {
      return HiveAuthConstants.AuthTypes.NONE.getAuthName();
    }
    for (HiveAuthConstants.AuthTypes type : PASSWORD_BASED_TYPES) {
      if (isEnabled(type)) {
        return type.getAuthName();
      }
    }
    return "";
  }
}
