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

import static org.apache.hadoop.hive.ql.parse.ParseUtils.ensureClassExists;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageFormat {

  private static final Logger LOG = LoggerFactory.getLogger(StorageFormat.class);
  private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();

  private final Configuration conf;
  private String inputFormat;
  private String outputFormat;
  private String storageHandler;
  private String serde;
  private final Map<String, String> serdeProps;

  public enum StorageHandlerTypes {
    DEFAULT(),
    ICEBERG("\'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler\'",
        "org.apache.iceberg.mr.hive.HiveIcebergInputFormat", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");

    private final String className;
    private final String inputFormat;
    private final String outputFormat;

    private StorageHandlerTypes() {
      this.className = null;
      this.inputFormat = null;
      this.outputFormat = null;
    }
    
    private StorageHandlerTypes(String className, String inputFormat, String outputFormat) {
      this.className = className;
      this.inputFormat = inputFormat;
      this.outputFormat = outputFormat;
    }

    public String className() {
      return className;
    }

    public String inputFormat() {
      return inputFormat;
    }

    public String outputFormat() {
      return outputFormat;
    }
  }

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
      for (int i = 0; i < child.getChildCount(); i++) {
        ASTNode grandChild = (ASTNode) child.getChild(i);
        switch (grandChild.getToken().getType()) {
          case HiveParser.TOK_FILEFORMAT_GENERIC:
            HiveStorageHandler handler;
            try {
              handler = HiveUtils.getStorageHandler(conf, storageHandler);
            } catch (HiveException e) {
              throw new SemanticException("Failed to load storage handler:  " + e.getMessage());
            }

            String fileFormatPropertyKey = handler.getFileFormatPropertyKey();
            if (fileFormatPropertyKey != null) {
              String fileFormat = grandChild.getChild(0).getText();
              if (serdeProps.containsKey(fileFormatPropertyKey)) {
                throw new SemanticException("Provide only one of the following: STORED BY " + fileFormat +
                    " or WITH SERDEPROPERTIES('" + fileFormatPropertyKey + "'='" + fileFormat + "')");
              }

              serdeProps.put(fileFormatPropertyKey, fileFormat);
            } else {
              throw new SemanticException("STORED AS is not supported for storage handler " +
                  handler.getClass().getName());
            }
            break;
          case HiveParser.TOK_TABLEPROPERTIES:
            BaseSemanticAnalyzer.readProps((ASTNode) grandChild.getChild(0), serdeProps);
            break;
          default:
            storageHandler = processStorageHandler(grandChild.getText());
        }
      }
      break;
    case HiveParser.TOK_FILEFORMAT_GENERIC:
      if (storageHandler != null) {
        // Under normal circumstances, we should not get here (since STORED BY and STORED AS are incompatible within the
        // same command). Only scenario we can end up here is if a default storage handler class is set in the config.
        // In this case, we opt to ignore the STORED AS clause.
        LOG.info("'STORED AS' clause will be ignored, since a default storage handler class is already set: '{}'",
            storageHandler);
        break;
      }
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

  private String processStorageHandler(String name) throws SemanticException {
    for (StorageHandlerTypes type : StorageHandlerTypes.values()) {
      if (type.name().equalsIgnoreCase(name)) {
        name = type.className();
        inputFormat = type.inputFormat();
        outputFormat = type.outputFormat();
        break;
      }
    }

    return ensureClassExists(BaseSemanticAnalyzer.unescapeSQLString(name));
  }

  public void processStorageFormat(String name) throws SemanticException {
    StorageFormatDescriptor descriptor = getDescriptor(name, "STORED AS clause");
    inputFormat = ensureClassExists(descriptor.getInputFormat());
    outputFormat = ensureClassExists(descriptor.getOutputFormat());
    if (serde == null) {
      serde = ensureClassExists(descriptor.getSerde());
    }
    if (serde == null) {
      // RCFile supports a configurable SerDe
      if (name.equalsIgnoreCase(IOConstants.RCFILE)) {
        serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_RCFILE_SERDE));
      } else {
        serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_SERDE));
      }
    }
  }

  public void fillDefaultStorageFormat(boolean isExternal, boolean isMaterializedView)
      throws  SemanticException {
    if ((inputFormat == null) && (storageHandler == null)) {
      String defaultFormat;
      String defaultManagedFormat;
      if (isMaterializedView) {
        defaultFormat = defaultManagedFormat =
            HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_FILE_FORMAT);
        serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_SERDE);
      } else {
        defaultFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_FILEFORMAT);
        defaultManagedFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_MANAGED_FILEFORMAT);
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
          serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DEFAULT_RCFILE_SERDE);
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

  public void setStorageHandler(String storageHandlerClass) throws SemanticException {
    storageHandler = ensureClassExists(storageHandlerClass);
  }

  public static StorageFormatDescriptor getDescriptor(String format, String clause) throws SemanticException {
    if (format.isEmpty()) {
      throw new SemanticException("File format in " + clause + " cannot be empty");
    }
    StorageFormatDescriptor descriptor = storageFormatFactory.get(format);
    if (descriptor == null) {
      throw new SemanticException("Unrecognized file format in " + clause + ":" + " '" + format + "'");
    }
    return descriptor;
  }
}
