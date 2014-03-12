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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This class is a place to put all the code associated with
 * Special cases. If there is a corner case required to make
 * a particular format work that is above and beyond the generic
 * use, it belongs here, for example. Over time, the goal is to
 * try to minimize usage of this, but it is a useful overflow
 * class that allows us to still be as generic as possible
 * in the main codeflow path, and call attention to the special
 * cases here.
 * Note : For all methods introduced here, please document why
 * the special case is necessary, providing a jira number if
 * possible.
 */
public class SpecialCases {

  static final private Log LOG = LogFactory.getLog(SpecialCases.class);

  /**
   * Method to do any file-format specific special casing while
   * instantiating a storage handler to write. We set any parameters
   * we want to be visible to the job in jobProperties, and this will
   * be available to the job via jobconf at run time.
   * @param jobProperties : map to write to
   * @param jobInfo : information about this output job to read from
   * @param ofclass : the output format in use
   */
  public static void addSpecialCasesParametersToOutputJobProperties(
      Map<String, String> jobProperties,
      OutputJobInfo jobInfo, Class<? extends OutputFormat> ofclass) {
    if (ofclass == RCFileOutputFormat.class) {
      // RCFile specific parameter
      jobProperties.put(RCFile.COLUMN_NUMBER_CONF_STR,
          Integer.toOctalString(
              jobInfo.getOutputSchema().getFields().size()));
    } else if (ofclass == OrcOutputFormat.class) {
      // Special cases for ORC
      // We need to check table properties to see if a couple of parameters,
      // such as compression parameters are defined. If they are, then we copy
      // them to job properties, so that it will be available in jobconf at runtime
      // See HIVE-5504 for details
      Map<String, String> tableProps = jobInfo.getTableInfo().getTable().getParameters();
      for (OrcFile.OrcTableProperties property : OrcFile.OrcTableProperties.values()){
        String propName = property.getPropName();
        if (tableProps.containsKey(propName)){
          jobProperties.put(propName,tableProps.get(propName));
        }
      }
    }
  }


}
