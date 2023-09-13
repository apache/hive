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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Identifier quotation.
 * @see HiveConf.ConfVars#HIVE_QUOTEDID_SUPPORT
 */
public enum Quotation {
  /**
   * Quotation of identifiers and special characters in identifiers are not allowed.
   * But regular expressions in backticks are supported for column names.
   */
  NONE("none", ""),
  /**
   * Use the backtick character to quote identifiers having special characters.
   * Use single quotes to quote string literals. Double quotes are also accepted but not recommended.
   */
  BACKTICKS("column", "`"),
  /**
   * SQL standard way to quote identifiers.
   * Use double quotes to quote identifiers having special characters and single quotes for string literals.
   */
  STANDARD("standard", "\"");

  Quotation(String stringValue, String quotationChar) {
    this.stringValue = stringValue;
    this.quotationChar = quotationChar;
  }

  private final String stringValue;
  private final String quotationChar;

  public String stringValue() {
    return stringValue;
  }

  public String getQuotationChar() {
    return quotationChar;
  }

  public static Quotation from(Configuration configuration) {
    String supportedQIds;
    if (configuration == null) {
      supportedQIds = HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.defaultStrVal;
    } else {
      supportedQIds = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
    }

    for (Quotation quotation : values()) {
      if (quotation.stringValue.equalsIgnoreCase(supportedQIds)) {
        return quotation;
      }
    }

    throw new EnumConstantNotPresentException(Quotation.class,
        "Option not recognized for " + HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname + "value: " + supportedQIds);
  }
}
