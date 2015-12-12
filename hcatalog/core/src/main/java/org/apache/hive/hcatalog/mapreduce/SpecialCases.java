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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.mapreduce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class is a place to put all the code associated with
 * Special cases. If there is a corner case required to make
 * a particular format work that is above and beyond the generic
 * use, it belongs here, for example. Over time, the goal is to
 * try to minimize usage of this, but it is a useful overflow
 * class that allows us to still be as generic as possible
 * in the main codeflow path, and call attention to the special
 * cases here.
 *
 * Note : For all methods introduced here, please document why
 * the special case is necessary, providing a jira number if
 * possible.
 */
public class SpecialCases {

  static final private Logger LOG = LoggerFactory.getLogger(SpecialCases.class);

  /**
   * Method to do any file-format specific special casing while
   * instantiating a storage handler to write. We set any parameters
   * we want to be visible to the job in jobProperties, and this will
   * be available to the job via jobconf at run time.
   *
   * This is mostly intended to be used by StorageHandlers that wrap
   * File-based OutputFormats such as FosterStorageHandler that wraps
   * RCFile, ORC, etc.
   *
   * @param jobProperties : map to write to
   * @param jobInfo : information about this output job to read from
   * @param ofclass : the output format in use
   */
  public static void addSpecialCasesParametersToOutputJobProperties(
      Map<String, String> jobProperties,
      OutputJobInfo jobInfo, Class<? extends OutputFormat> ofclass) {
    if (ofclass == RCFileOutputFormat.class) {
      // RCFile specific parameter
      jobProperties.put(HiveConf.ConfVars.HIVE_RCFILE_COLUMN_NUMBER_CONF.varname,
          Integer.toOctalString(
              jobInfo.getOutputSchema().getFields().size()));
    } else if (ofclass == OrcOutputFormat.class) {
      // Special cases for ORC
      // We need to check table properties to see if a couple of parameters,
      // such as compression parameters are defined. If they are, then we copy
      // them to job properties, so that it will be available in jobconf at runtime
      // See HIVE-5504 for details
      Map<String, String> tableProps = jobInfo.getTableInfo().getTable().getParameters();
      for (OrcConf property : OrcConf.values()){
        String propName = property.getAttribute();
        if (tableProps.containsKey(propName)){
          jobProperties.put(propName, tableProps.get(propName));
        }
      }
    } else if (ofclass == AvroContainerOutputFormat.class) {
      // Special cases for Avro. As with ORC, we make table properties that
      // Avro is interested in available in jobconf at runtime
      Map<String, String> tableProps = jobInfo.getTableInfo().getTable().getParameters();
      for (AvroSerdeUtils.AvroTableProperties property : AvroSerdeUtils.AvroTableProperties.values()) {
        String propName = property.getPropName();
        if (tableProps.containsKey(propName)){
          String propVal = tableProps.get(propName);
          jobProperties.put(propName,tableProps.get(propName));
        }
      }

      Properties properties = new Properties();
      properties.put("name",jobInfo.getTableName());

      List<String> colNames = jobInfo.getOutputSchema().getFieldNames();
      List<TypeInfo> colTypes = new ArrayList<TypeInfo>();
      for (HCatFieldSchema field : jobInfo.getOutputSchema().getFields()){
        colTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getTypeString()));
      }

      if (jobProperties.get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())==null
          || jobProperties.get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()).isEmpty()) {
     
        jobProperties.put(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
          AvroSerDe.getSchemaFromCols(properties, colNames, colTypes, null).toString());
      }


    }
  }

  /**
   * Method to do any storage-handler specific special casing while instantiating a
   * HCatLoader
   *
   * @param conf : configuration to write to
   * @param tableInfo : the table definition being used
   */
  public static void addSpecialCasesParametersForHCatLoader(
      Configuration conf, HCatTableInfo tableInfo) {
    if ((tableInfo == null) || (tableInfo.getStorerInfo() == null)){
      return;
    }
    String shClass = tableInfo.getStorerInfo().getStorageHandlerClass();
    if ((shClass != null) && shClass.equals("org.apache.hadoop.hive.hbase.HBaseStorageHandler")){
      // NOTE: The reason we use a string name of the hive hbase handler here is
      // because we do not want to introduce a compile-dependency on the hive-hbase-handler
      // module from within hive-hcatalog.
      // This parameter was added due to the requirement in HIVE-7072
      conf.set("pig.noSplitCombination", "true");
    }
  }

}
