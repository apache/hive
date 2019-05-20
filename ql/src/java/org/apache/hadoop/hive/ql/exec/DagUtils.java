/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import com.google.common.base.Strings;

public class DagUtils {

  public static final String MAPREDUCE_WORKFLOW_NODE_NAME = "mapreduce.workflow.node.name";

  public static String getQueryName(Configuration conf) {
    String name = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYNAME);
    if (Strings.isNullOrEmpty(name)) {
      return conf.get(MRJobConfig.JOB_NAME);
    } else {
      return name + " (" + conf.get(DagUtils.MAPREDUCE_WORKFLOW_NODE_NAME) + ")";
    }
  }

}
