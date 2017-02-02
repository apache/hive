/**
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

import static org.apache.hadoop.hive.ql.parse.ParseUtils.ensureClassExists;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;

public class StorageFormat {
  private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
  private final Configuration conf;
  private String inputFormat;
  private String outputFormat;
  private String storageHandler;
  private String serde;
  private final Map<String, String> serdeProps;

  public StorageFormat(Configuration conf) {
    this.conf = conf;
    this.serdeProps = new HashMap<String, String>();
  }

  /**
   * Returns true if the passed token was a storage format token
   * and thus was processed accordingly.
   */
  public boolean fillStorageFormat(ASTNode child) throws SemanticException {
    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABLEFILEFORMAT:
      if (child.getChildCount() < 2) {
        throw new SemanticException(
          "Incomplete specification of File Format. " +
            "You must provide InputFormat, OutputFormat.");
      }
      inputFormat = ensureClassExists(BaseSemanticAnalyzer.unescapeSQLString(child.getChild(0).getText()));
      outputFormat = ensureClassExists(BaseSemanticAnalyzer.unescapeSQLString(child.getChild(1).getText()));
      if (child.getChildCount() == 3) {
        serde = ensureClassExists(BaseSemanticAnalyzer.unescapeSQLString(child.getChild(2).getText()));
      }
      break;
    case HiveParser.TOK_STORAGEHANDLER:
      storageHandler = ensureClassExists(BaseSemanticAnalyzer.unescapeSQLString(child.getChild(0).getText()));
      if (child.getChildCount() == 2) {
        BaseSemanticAnalyzer.readProps(
          (ASTNode) (child.getChild(1).getChild(0)),
          serdeProps);
      }
      break;
    case HiveParser.TOK_FILEFORMAT_GENERIC:
      ASTNode grandChild = (ASTNode)child.getChild(0);
      String name = (grandChild == null ? "" : grandChild.getText()).trim().toUpperCase();
      processStorageFormat(name);
      break;
    default:
      // token was not a storage format token
      return false;
    }
    return true;
  }

  protected void processStorageFormat(String name) throws SemanticException {
    if (name.isEmpty()) {
      throw new SemanticException("File format in STORED AS clause cannot be empty");
    }
    StorageFormatDescriptor descriptor = storageFormatFactory.get(name);
    if (descriptor == null) {
      throw new SemanticException("Unrecognized file format in STORED AS clause:" +
          " '" + name + "'");
    }
    inputFormat = ensureClassExists(descriptor.getInputFormat());
    outputFormat = ensureClassExists(descriptor.getOutputFormat());
    if (serde == null) {
      serde = ensureClassExists(descriptor.getSerde());
    }
    if (serde == null) {
      // RCFile supports a configurable SerDe
      if (name.equalsIgnoreCase(IOConstants.RCFILE)) {
        serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE));
      } else {
        serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTSERDE));
      }
    }
  }

  protected void fillDefaultStorageFormat(boolean isExternal, boolean isMaterializedView)
      throws  SemanticException {
    if ((inputFormat == null) && (storageHandler == null)) {
      String defaultFormat;
      String defaultManagedFormat;
      if (isMaterializedView) {
        defaultFormat = defaultManagedFormat =
            HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_FILE_FORMAT);
        serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_SERDE);
      } else {
        defaultFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
        defaultManagedFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTMANAGEDFILEFORMAT);
      }

      if (!isExternal && !"none".equals(defaultManagedFormat)) {
        defaultFormat = defaultManagedFormat;
      }

      if (StringUtils.isBlank(defaultFormat)) {
        inputFormat = IOConstants.TEXTFILE_INPUT;
        outputFormat = IOConstants.TEXTFILE_OUTPUT;
      } else {
        processStorageFormat(defaultFormat);
        if (defaultFormat.equalsIgnoreCase(IOConstants.RCFILE)) {
          serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
        }
      }
    }
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public String getStorageHandler() {
    return storageHandler;
  }

  public String getSerde() {
    return serde;
  }

  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }
}
