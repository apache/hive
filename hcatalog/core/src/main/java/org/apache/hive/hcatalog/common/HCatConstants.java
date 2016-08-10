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
package org.apache.hive.hcatalog.common;

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
  public static final String HCAT_PIG_LOADER_LOCATION_SET = HCAT_PIG_LOADER + ".location.set";
  public static final String HCAT_PIG_LOADER_ARGS = "hcat.pig.loader.args";
  public static final String HCAT_PIG_STORER = "hcat.pig.storer";
  public static final String HCAT_PIG_STORER_ARGS = "hcat.pig.storer.args";
  public static final String HCAT_PIG_ARGS_DELIMIT = "hcat.pig.args.delimiter";
  public static final String HCAT_PIG_ARGS_DELIMIT_DEFAULT = ",";
  public static final String HCAT_PIG_STORER_LOCATION_SET = HCAT_PIG_STORER + ".location.set";
  public static final String HCAT_PIG_INNER_TUPLE_NAME = "hcat.pig.inner.tuple.name";
  public static final String HCAT_PIG_INNER_TUPLE_NAME_DEFAULT = "innertuple";
  public static final String HCAT_PIG_INNER_FIELD_NAME = "hcat.pig.inner.field.name";
  public static final String HCAT_PIG_INNER_FIELD_NAME_DEFAULT = "innerfield";

  /**
   * {@value} (default: null)
   * When the property is set in the UDFContext of the org.apache.hive.hcatalog.pig.HCatStorer, HCatStorer writes
   * to the location it specifies instead of the default HCatalog location format. An example can be found
   * in org.apache.hive.hcatalog.pig.HCatStorerWrapper.
   */
  public static final String HCAT_PIG_STORER_EXTERNAL_LOCATION = HCAT_PIG_STORER + ".external.location";

  //The keys used to store info into the job Configuration
  public static final String HCAT_KEY_BASE = "mapreduce.lib.hcat";

  public static final String HCAT_KEY_OUTPUT_SCHEMA = HCAT_KEY_BASE + ".output.schema";

  public static final String HCAT_KEY_JOB_INFO = HCAT_KEY_BASE + ".job.info";

  // hcatalog specific configurations, that can be put in hive-site.xml
  public static final String HCAT_HIVE_CLIENT_EXPIRY_TIME = "hcatalog.hive.client.cache.expiry.time";

  // config parameter that suggests to hcat that metastore clients not be cached - default is false
  // this parameter allows highly-parallel hcat usescases to not gobble up too many connections that
  // sit in the cache, while not in use.
  public static final String HCAT_HIVE_CLIENT_DISABLE_CACHE = "hcatalog.hive.client.cache.disabled";

  // Indicates the initial capacity of the cache.
  public static final String HCAT_HIVE_CLIENT_CACHE_INITIAL_CAPACITY = "hcatalog.hive.client.cache.initial.capacity";

  // Indicates the maximum capacity of the cache. Minimum value should be the number of threads.
  public static final String HCAT_HIVE_CLIENT_CACHE_MAX_CAPACITY = "hcatalog.hive.client.cache.max.capacity";

  // Indicates whether cache statistics should be collected.
  public static final String HCAT_HIVE_CLIENT_CACHE_STATS_ENABLED = "hcatalog.hive.client.cache.stats.enabled";

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

  /**
   * The desired number of input splits produced for each partition. When the
   * input files are large and few, we want to split them into many splits,
   * so as to increase the parallelizm of loading the splits. Try also two
   * other parameters, mapred.min.split.size and mapred.max.split.size for
   * hadoop 1.x, or mapreduce.input.fileinputformat.split.minsize and
   * mapreduce.input.fileinputformat.split.maxsize in hadoop 2.x to
   * control the number of input splits.
   */
  public static final String HCAT_DESIRED_PARTITION_NUM_SPLITS =
    "hcat.desired.partition.num.splits";

  /**
   * hcat.append.limit allows a hcat user to specify a custom append limit.
   * By default, while appending to an existing directory, hcat will attempt
   * to avoid naming clashes and try to append _a_NNN where NNN is a number to
   * the desired filename to avoid clashes. However, by default, it only tries
   * for NNN from 0 to 999 before giving up. This can cause an issue for some
   * tables with an extraordinarily large number of files. Ideally, this should
   * be fixed by the user changing their usage pattern and doing some manner of
   * compaction, but in the meanwhile, until they can, setting this parameter
   * can be used to bump that limit.
   */
  public static final String HCAT_APPEND_LIMIT = "hcat.append.limit";

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

  public static final String[] OUTPUT_CONFS_TO_SAVE = {
    HCAT_KEY_OUTPUT_INFO,
    HCAT_KEY_HIVE_CONF,
    HCAT_KEY_TOKEN_SIGNATURE
  };


  public static final String HCAT_MSG_CLEAN_FREQ = "hcat.msg.clean.freq";
  public static final String HCAT_MSG_EXPIRY_DURATION = "hcat.msg.expiry.duration";

  public static final String HCAT_MSGBUS_TOPIC_NAME = "hcat.msgbus.topic.name";
  public static final String HCAT_MSGBUS_TOPIC_NAMING_POLICY = "hcat.msgbus.topic.naming.policy";
  public static final String HCAT_MSGBUS_TOPIC_PREFIX = "hcat.msgbus.topic.prefix";

  public static final String HCAT_OUTPUT_ID_HASH = HCAT_KEY_OUTPUT_BASE + ".id";

  public static final String HCAT_DYNAMIC_PTN_JOBID = HCAT_KEY_OUTPUT_BASE + ".dynamic.jobid";
  public static final boolean HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED = false;
  public static final String HCAT_DYNAMIC_CUSTOM_PATTERN = "hcat.dynamic.partitioning.custom.pattern";

  // Message Bus related properties.
  public static final String HCAT_DEFAULT_TOPIC_PREFIX = "hcat";
  public static final String HCAT_EVENT = "HCAT_EVENT";
  public static final String HCAT_ADD_PARTITION_EVENT = "ADD_PARTITION";
  public static final String HCAT_DROP_PARTITION_EVENT = "DROP_PARTITION";
  public static final String HCAT_ALTER_PARTITION_EVENT = "ALTER_PARTITION";
  public static final String HCAT_PARTITION_DONE_EVENT = "PARTITION_DONE";
  public static final String HCAT_CREATE_TABLE_EVENT = "CREATE_TABLE";
  public static final String HCAT_ALTER_TABLE_EVENT = "ALTER_TABLE";
  public static final String HCAT_DROP_TABLE_EVENT = "DROP_TABLE";
  public static final String HCAT_CREATE_DATABASE_EVENT = "CREATE_DATABASE";
  public static final String HCAT_DROP_DATABASE_EVENT = "DROP_DATABASE";
  public static final String HCAT_CREATE_FUNCTION_EVENT = "CREATE_FUNCTION";
  public static final String HCAT_DROP_FUNCTION_EVENT = "DROP_FUNCTION";
  public static final String HCAT_CREATE_INDEX_EVENT = "CREATE_INDEX";
  public static final String HCAT_DROP_INDEX_EVENT = "DROP_INDEX";
  public static final String HCAT_ALTER_INDEX_EVENT = "ALTER_INDEX";
  public static final String HCAT_INSERT_EVENT = "INSERT";
  public static final String HCAT_MESSAGE_VERSION = "HCAT_MESSAGE_VERSION";
  public static final String HCAT_MESSAGE_FORMAT = "HCAT_MESSAGE_FORMAT";
  public static final String CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX = "hcatalog.message.factory.impl.";
  public static final String CONF_LABEL_HCAT_MESSAGE_FORMAT = "hcatalog.message.format";
  public static final String DEFAULT_MESSAGE_FACTORY_IMPL = "org.apache.hive.hcatalog.messaging.json.JSONMessageFactory";

  // System environment variables
  public static final String SYSENV_HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";

  // Hadoop Conf Var Names
  public static final String CONF_MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

  //***************************************************************************
  // Data-related configuration properties.
  //***************************************************************************

  /**
   * {@value} (default: {@value #HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER_DEFAULT}).
   * Pig < 0.10.0 does not have boolean support, and scripts written for pre-boolean Pig versions
   * will not expect boolean values when upgrading Pig. For integration the option is offered to
   * convert boolean fields to integers by setting this Hadoop configuration key.
   */
  public static final String HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER =
    "hcat.data.convert.boolean.to.integer";
  public static final boolean HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER_DEFAULT = false;

  /**
   * {@value} (default: {@value #HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT}).
   * Hive tables support tinyint and smallint columns, while not all processing frameworks support
   * these types (Pig only has integer for example). Enable this property to promote tinyint and
   * smallint columns to integer at runtime. Note that writes to tinyint and smallint columns
   * enforce bounds checking and jobs will fail if attempting to write values outside the column
   * bounds.
   */
  public static final String HCAT_DATA_TINY_SMALL_INT_PROMOTION =
    "hcat.data.tiny.small.int.promotion";
  public static final boolean HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT = false;

  /**
   * {@value} (default: {@value #HCAT_INPUT_BAD_RECORD_THRESHOLD_DEFAULT}).
   * Threshold for the ratio of bad records that will be silently skipped without causing a task
   * failure. This is useful when processing large data sets with corrupt records, when its
   * acceptable to skip some bad records.
   */
  public static final String HCAT_INPUT_BAD_RECORD_THRESHOLD_KEY = "hcat.input.bad.record.threshold";
  public static final float HCAT_INPUT_BAD_RECORD_THRESHOLD_DEFAULT = 0.0001f;

  /**
   * {@value} (default: {@value #HCAT_INPUT_BAD_RECORD_MIN_DEFAULT}).
   * Number of bad records that will be accepted before applying
   * {@value #HCAT_INPUT_BAD_RECORD_THRESHOLD_KEY}. This is necessary to prevent an initial bad
   * record from causing a task failure.
   */
  public static final String HCAT_INPUT_BAD_RECORD_MIN_KEY = "hcat.input.bad.record.min";
  public static final int HCAT_INPUT_BAD_RECORD_MIN_DEFAULT = 2;

  public static final String HCAT_INPUT_IGNORE_INVALID_PATH_KEY = "hcat.input.ignore.invalid.path";
  public static final boolean HCAT_INPUT_IGNORE_INVALID_PATH_DEFAULT = false;
}
