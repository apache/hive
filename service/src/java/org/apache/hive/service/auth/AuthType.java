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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AuthType is used to parse and verify
 * {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#HIVE_SERVER2_AUTHENTICATION}.
 * Throws an exception if the config value is not allowed.
 */
public class AuthType {
  static final Set<HiveAuthConstants.AuthTypes> PASSWORD_BASED_TYPES = ImmutableSet.of(HiveAuthConstants.AuthTypes.LDAP,
      HiveAuthConstants.AuthTypes.CUSTOM, HiveAuthConstants.AuthTypes.PAM, HiveAuthConstants.AuthTypes.NONE);
  private final BitSet typeBits;
  private final List<HiveAuthConstants.AuthTypes> authTypes;
  private final HiveServer2TransportMode mode;

  @VisibleForTesting
  public AuthType(String authTypes, HiveServer2TransportMode mode) {
    this.authTypes = new ArrayList<>();
    this.mode = mode;
    typeBits = new BitSet();
    parseTypes(authTypes);
    verifyTypes(authTypes);
  }

  private void parseTypes(String authTypes) {
    String[] types = authTypes.split(",");
    for (String type : types) {
      if (!EnumUtils.isValidEnumIgnoreCase(HiveAuthConstants.AuthTypes.class, type)) {
        throw new IllegalArgumentException(type + " is not a valid authentication type.");
      }
      HiveAuthConstants.AuthTypes authType = EnumUtils.getEnumIgnoreCase(HiveAuthConstants.AuthTypes.class, type);
      this.authTypes.add(authType);
      typeBits.set(authType.ordinal());
    }
  }

  private void verifyTypes(String authTypes) {
    if (typeBits.cardinality() == 1) {
      // single authentication type has no conflicts
      return;
    }
    if (typeBits.get(HiveAuthConstants.AuthTypes.NOSASL.ordinal())) {
      throw new UnsupportedOperationException("NOSASL can't be along with other auth methods: " + authTypes);
    }

    if (typeBits.get(HiveAuthConstants.AuthTypes.NONE.ordinal())) {
      throw new UnsupportedOperationException("None can't be along with other auth methods: " + authTypes);
    }

    if (areAnyEnabled(PASSWORD_BASED_TYPES) && !isExactlyOneEnabled(PASSWORD_BASED_TYPES)) {
      throw new RuntimeException("Multiple password based auth methods found: " + authTypes);
    }

    if ((typeBits.get(HiveAuthConstants.AuthTypes.SAML.ordinal()) ||
        typeBits.get(HiveAuthConstants.AuthTypes.JWT.ordinal())) &&
        (mode == HiveServer2TransportMode.all || mode == HiveServer2TransportMode.binary)) {
      throw new UnsupportedOperationException(
          "HiveServer2 binary mode doesn't support JWT and SAML," + " please consider using http mode only");
    }
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

  public boolean isPasswordBasedAuthEnabled() {
    return areAnyEnabled(PASSWORD_BASED_TYPES);
  }

  public String getAuthTypes() {
    return authTypes.stream().map(au -> au.getAuthName()).collect(Collectors.joining(","));
  }

  public String getPasswordBasedAuthStr() {
    if (isEnabled(HiveAuthConstants.AuthTypes.NOSASL)) {
      return HiveAuthConstants.AuthTypes.NOSASL.getAuthName();
    }
    for (HiveAuthConstants.AuthTypes type : PASSWORD_BASED_TYPES) {
      if (isEnabled(type)) {
        return type.getAuthName();
      }
    }
    return "";
  }

  public boolean isPasswordBasedAuth(HiveAuthConstants.AuthTypes type) {
    return PASSWORD_BASED_TYPES.contains(type);
  }

  /**
   * Refer from configuration to see if Kerberos auth method is enabled
   * @return true if kerberos is enabled, otherwise false.
   */
  public static boolean isKerberosAuthMode(Configuration conf) {
    AuthType authType = authTypeFromConf(conf, true);
    return authType.isEnabled(HiveAuthConstants.AuthTypes.KERBEROS);
  }

  /**
   * Refer from configuration to see if SAML auth method is enabled
   * @return true if SAML is enabled, otherwise false.
   */
  public static boolean isSamlAuthMode(Configuration conf) {
    AuthType authType = authTypeFromConf(conf, true);
    return authType.isEnabled(HiveAuthConstants.AuthTypes.SAML);
  }

  public static AuthType authTypeFromConf(Configuration conf, boolean isHttpMode) {
    String authTypeStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);
    boolean isAuthTypeEmpty = StringUtils.isEmpty(authTypeStr);
    final HiveServer2TransportMode transportMode;
    if (isHttpMode) {
      transportMode = HiveServer2TransportMode.http;
      if (isAuthTypeEmpty) {
        authTypeStr = HiveAuthConstants.AuthTypes.NOSASL.getAuthName();
      }
    } else {
      transportMode = HiveServer2TransportMode.binary;
      if (isAuthTypeEmpty) {
        authTypeStr = HiveAuthConstants.AuthTypes.NONE.getAuthName();
      }
    }
    return new AuthType(authTypeStr, transportMode);
  }

}
