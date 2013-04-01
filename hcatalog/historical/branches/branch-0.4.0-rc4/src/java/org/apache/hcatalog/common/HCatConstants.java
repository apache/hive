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
package org.apache.hcatalog.common;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public final class HCatConstants {

  public static final String HIVE_RCFILE_IF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
  public static final String HIVE_RCFILE_OF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";

  public static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class.getName();
  public static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class.getName();
  
  public static final String HCAT_PIG_STORAGE_CLASS = "org.apache.pig.builtin.PigStorage";
  public static final String HCAT_PIG_LOADER = "hcat.pig.loader";
  public static final String HCAT_PIG_LOADER_ARGS = "hcat.pig.loader.args";
  public static final String HCAT_PIG_STORER = "hcat.pig.storer";
  public static final String HCAT_PIG_STORER_ARGS = "hcat.pig.storer.args";
  public static final String HCAT_PIG_ARGS_DELIMIT = "hcat.pig.args.delimiter";
  public static final String HCAT_PIG_ARGS_DELIMIT_DEFAULT = ",";
  
  //The keys used to store info into the job Configuration
  public static final String HCAT_KEY_BASE = "mapreduce.lib.hcat";

  public static final String HCAT_KEY_OUTPUT_SCHEMA = HCAT_KEY_BASE + ".output.schema";

  public static final String HCAT_KEY_JOB_INFO =  HCAT_KEY_BASE + ".job.info";

  private HCatConstants() { // restrict instantiation
  }

  public static final String HCAT_TABLE_SCHEMA = "hcat.table.schema";

  public static final String HCAT_METASTORE_URI = HiveConf.ConfVars.METASTOREURIS.varname;

  public static final String HCAT_PERMS = "hcat.perms";

  public static final String HCAT_GROUP = "hcat.group";

  public static final String HCAT_CREATE_TBL_NAME = "hcat.create.tbl.name";

  public static final String HCAT_CREATE_DB_NAME = "hcat.create.db.name";

  public static final String HCAT_METASTORE_PRINCIPAL 
          = HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname;

  // IMPORTANT IMPORTANT IMPORTANT!!!!!
  //The keys used to store info into the job Configuration.
  //If any new keys are added, the HCatStorer needs to be updated. The HCatStorer
  //updates the job configuration in the backend to insert these keys to avoid
  //having to call setOutput from the backend (which would cause a metastore call
  //from the map jobs)
  public static final String HCAT_KEY_OUTPUT_BASE = "mapreduce.lib.hcatoutput";
  public static final String HCAT_KEY_OUTPUT_INFO = HCAT_KEY_OUTPUT_BASE + ".info";
  public static final String HCAT_KEY_HIVE_CONF = HCAT_KEY_OUTPUT_BASE + ".hive.conf";
  public static final String HCAT_KEY_TOKEN_SIGNATURE = HCAT_KEY_OUTPUT_BASE + ".token.sig";
  public static final String HCAT_KEY_JOBCLIENT_TOKEN_SIGNATURE = HCAT_KEY_OUTPUT_BASE + ".jobclient.token.sig";
  public static final String HCAT_KEY_JOBCLIENT_TOKEN_STRFORM = HCAT_KEY_OUTPUT_BASE + ".jobclient.token.strform";

  public static final String[] OUTPUT_CONFS_TO_SAVE = {
    HCAT_KEY_OUTPUT_INFO,
    HCAT_KEY_HIVE_CONF,
    HCAT_KEY_TOKEN_SIGNATURE,
    HCAT_KEY_JOBCLIENT_TOKEN_SIGNATURE,
    HCAT_KEY_JOBCLIENT_TOKEN_STRFORM
    };


  public static final String HCAT_MSG_CLEAN_FREQ = "hcat.msg.clean.freq";
  public static final String HCAT_MSG_EXPIRY_DURATION = "hcat.msg.expiry.duration";
  
  public static final String HCAT_MSGBUS_TOPIC_NAME = "hcat.msgbus.topic.name";
  public static final String HCAT_MSGBUS_TOPIC_NAMING_POLICY = "hcat.msgbus.topic.naming.policy";
  public static final String HCAT_MSGBUS_TOPIC_PREFIX = "hcat.msgbus.topic.prefix";
  
  public static final String HCAT_DYNAMIC_PTN_JOBID = HCAT_KEY_OUTPUT_BASE + "dynamic.jobid";
  public static final boolean HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED = false;

  // Message Bus related properties.
  public static final String HCAT_DEFAULT_TOPIC_PREFIX = "hcat";
  public static final String HCAT_EVENT = "HCAT_EVENT";
  public static final String HCAT_ADD_PARTITION_EVENT = "HCAT_ADD_PARTITION";
  public static final String HCAT_DROP_PARTITION_EVENT = "HCAT_DROP_PARTITION";
  public static final String HCAT_PARTITION_DONE_EVENT = "HCAT_PARTITION_DONE";
  public static final String HCAT_ADD_TABLE_EVENT = "HCAT_ADD_TABLE";
  public static final String HCAT_DROP_TABLE_EVENT = "HCAT_DROP_TABLE";
  public static final String HCAT_ADD_DATABASE_EVENT = "HCAT_ADD_DATABASE";
  public static final String HCAT_DROP_DATABASE_EVENT = "HCAT_DROP_DATABASE";

  // System environment variables
  public static final String SYSENV_HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";
  
  // Hadoop Conf Var Names
  public static final String CONF_MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

}
