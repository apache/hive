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
package org.apache.hadoop.hive.serde2.text;

import java.util.EnumSet;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;

import com.google.common.base.Preconditions;

@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS,
    serdeConstants.SERIALIZATION_ENCODING, AbstractTextSerDe.IGNORE_EMPTY_LINES,
    RegexTextSerDe.INPUT_REGEX, RegexTextSerDe.INPUT_REGEX_CASE_SENSITIVE })
public class RegexTextSerDe extends AbstractRegexTextSerDe {

  public static final String INPUT_REGEX = "input.regex";
  public static final String INPUT_REGEX_CASE_SENSITIVE =
      "input.regex.case.insensitive";
  public static final String STRICT_FIELDS_COUNT = "text.strict.fields.count";

  private Pattern pattern;

  private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

  /**
   * Enumeration that defines all on/off features for this SerDe.
   * <ul>
   * <li>{@link #CHECK_FIELD_COUNT}</li>
   * </ul>
   */
  public enum Feature {
    /**
     * If this feature is enable, text will be considered by the regex with
     * respect for case.
     */
    CASE_INSENSITIVE_REGEX
  }

  @Override
  public void initialize(final Configuration configuration,
      final Properties tableProperties) throws SerDeException {

    if (Boolean.valueOf(
        tableProperties.getProperty(INPUT_REGEX_CASE_SENSITIVE, "false"))) {
      features.add(Feature.CASE_INSENSITIVE_REGEX);
    }

    final String regex = tableProperties.getProperty(INPUT_REGEX, "");
    Preconditions.checkArgument(!regex.isEmpty());

    LOG.debug("SerDe configured with regex: {}", regex);

    this.pattern = features.contains(Feature.CASE_INSENSITIVE_REGEX)
        ? Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
        : Pattern.compile(regex);

    super.setPattern(this.pattern);
    super.initialize(configuration, tableProperties);
  }
}
