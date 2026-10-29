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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The allow list bounding which configuration parameters a table property may override on a compaction
 * job, read from {@link HiveConf.ConfVars#HIVE_COMPACTOR_CONF_ALLOWLIST}. This config limits the scope
 * of the overridable config parameters, because most of the parameters have no effect on a compaction job.
 * With this limitation, the regex defined by the deny list can be kept simple and short.
 * <p>
 * The config parameters are first evaluated against the allow list config. If they match, then they will be
 * evaluated against the deny list, read from {@link HiveConf.ConfVars#HIVE_COMPACTOR_CONF_DENYLIST}.
 * The purpose of the deny list is to filter for parameters which could lead to security gaps.
 * <p>
 * The admins can extend the allow list through {@link HiveConf.ConfVars#HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND}.
 * The config parameters listed here are also evaluated against the deny list to prevent accidentally allowing
 * configs which hold security risk.
 * <p>
 * As last step, all config parameters are checked against the already existing lists such as
 * {@link HiveConf.ConfVars#HIVE_CONF_RESTRICTED_LIST}, {@link HiveConf.ConfVars#HIVE_CONF_HIDDEN_LIST}
 * and {@link HiveConf.ConfVars#HIVE_CONF_LOCKED_LIST}
 * This applies for the parameters defined through {@link HiveConf.ConfVars#HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND}
 * as well.
 */
final class CompactorPropertyValidator {

  private static final Logger LOG = LoggerFactory.getLogger(CompactorPropertyValidator.class);
  private static final Map<String, Pattern> ALLOW_PATTERN_CACHE = new ConcurrentHashMap<>();
  private static final Map<String, Pattern> DENY_PATTERN_CACHE = new ConcurrentHashMap<>();

  private static final Pattern MATCHES_NOTHING = Pattern.compile("(?!)");
  private static final Pattern DENY_EVERYTHING = Pattern.compile(".*");

  private CompactorPropertyValidator() {
  }

  /**
   * Decides whether a parameter is within the set a table property may override.
   *
   * @param conf configuration the allow list settings are read from
   * @param parameterName the config parameter name with its "compactor." or "tblprops." prefix already removed
   */
  static boolean isParameterAllowed(Configuration conf, String parameterName) {
    String key = parameterName.trim();
    if (StringUtils.isBlank(key)) {
      return false;
    }

    Pattern allowPattern =
        getAllowPattern(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST), MATCHES_NOTHING,
            HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST.varname);

    if ((allowPattern != null && allowPattern.matcher(key).find()) || isOnAllowListAppend(conf, key)) {
      Pattern denyPattern =
          getDenyPattern(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_CONF_DENYLIST), DENY_EVERYTHING,
              HiveConf.ConfVars.HIVE_COMPACTOR_CONF_DENYLIST.varname);
      if (denyPattern != null && denyPattern.matcher(key).find()) {
        LOG.warn("Ignoring compaction property '{}', as it is not permitted to override by table properties per {}.",
            key, HiveConf.ConfVars.HIVE_COMPACTOR_CONF_DENYLIST.varname);
        return false;
      }

      if (matchesAnyPrefix(HiveConfUtil.getHiddenSet(conf), key) || matchesAnyPrefix(HiveConfUtil.getLockedSet(conf),
          key) || matchesAnyPrefix(HiveConfUtil.getRestrictedSet(conf), key)) {
        LOG.warn("Ignoring compaction property '{}' as it is immutable at runtime per {}, {} or {}.", key,
            HiveConf.ConfVars.HIVE_CONF_RESTRICTED_LIST.varname, HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname,
            HiveConf.ConfVars.HIVE_CONF_LOCKED_LIST.varname);
        return false;
      }
      return true;
    } else {
      LOG.warn("Ignoring compaction property '{}', as it is not allowed by {} or {}.", key,
          HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST.varname,
          HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND.varname);
      return false;
    }
  }

  private static boolean isOnAllowListAppend(Configuration conf, String key) {
    String configured = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND);
    if (StringUtils.isBlank(configured)) {
      return false;
    }
    for (String exception : configured.split(",")) {
      if (key.equalsIgnoreCase(exception.trim())) {
        return true;
      }
    }
    return false;
  }

  private static Pattern getAllowPattern(String regexConfig, Pattern fallbackRegex, String configName) {
    return getPattern(regexConfig, fallbackRegex, configName, ALLOW_PATTERN_CACHE);
  }

  private static Pattern getDenyPattern(String regexConfig, Pattern fallbackRegex, String configName) {
    return getPattern(regexConfig, fallbackRegex, configName, DENY_PATTERN_CACHE);
  }

  private static Pattern getPattern(String regexConfig, Pattern fallbackRegex, String configName,
      Map<String, Pattern> PATTERN_CACHE) {

    String regex = StringUtils.trimToEmpty(regexConfig);

    if (regex.isEmpty()) {
      return null;
    }
    return PATTERN_CACHE.computeIfAbsent(regex, r -> {
      try {
        return Pattern.compile(r, Pattern.CASE_INSENSITIVE);
      } catch (PatternSyntaxException e) {
        LOG.error("Invalid regex '{}' is set in '{}'. No property will be applied until it is fixed.", r, configName,
            e);
        return fallbackRegex;
      }
    });
  }

  private static boolean matchesAnyPrefix(Set<String> prefixes, String key) {
    return prefixes.stream().anyMatch(prefix -> !prefix.isEmpty() && key.startsWith(prefix));
  }
}


