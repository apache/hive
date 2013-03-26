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

public final class HCatConstants {

  /** The key for the input storage driver class name */
  public static final String HCAT_ISD_CLASS = "hcat.isd";

  /** The key for the output storage driver class name */
  public static final String HCAT_OSD_CLASS = "hcat.osd";

  public static final String HIVE_RCFILE_IF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
  public static final String HIVE_RCFILE_OF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";
  public static final String HCAT_RCFILE_ISD_CLASS = "org.apache.hcatalog.rcfile.RCFileInputDriver";
  public static final String HCAT_RCFILE_OSD_CLASS = "org.apache.hcatalog.rcfile.RCFileOutputDriver";

  //The keys used to store info into the job Configuration
  public static final String HCAT_KEY_BASE = "mapreduce.lib.hcat";

  public static final String HCAT_KEY_OUTPUT_SCHEMA = HCAT_KEY_BASE + ".output.schema";

  public static final String HCAT_KEY_JOB_INFO =  HCAT_KEY_BASE + ".job.info";

  private HCatConstants() { // restrict instantiation
  }

  public static final String HCAT_TABLE_SCHEMA = "hcat.table.schema";

  public static final String HCAT_METASTORE_URI = "hcat.metastore.uri";

  public static final String HCAT_PERMS = "hcat.perms";

  public static final String HCAT_GROUP = "hcat.group";

  public static final String HCAT_CREATE_TBL_NAME = "hcat.create.tbl.name";

  public static final String HCAT_CREATE_DB_NAME = "hcat.create.db.name";

  public static final String HCAT_METASTORE_PRINCIPAL = "hcat.metastore.principal";

  // IMPORTANT IMPORTANT IMPORTANT!!!!!
  //The keys used to store info into the job Configuration.
  //If any new keys are added, the HowlStorer needs to be updated. The HowlStorer
  //updates the job configuration in the backend to insert these keys to avoid
  //having to call setOutput from the backend (which would cause a metastore call
  //from the map jobs)
  public static final String HCAT_KEY_OUTPUT_BASE = "mapreduce.lib.hcatoutput";
  public static final String HCAT_KEY_OUTPUT_INFO = HCAT_KEY_OUTPUT_BASE + ".info";
  public static final String HCAT_KEY_HIVE_CONF = HCAT_KEY_OUTPUT_BASE + ".hive.conf";
  public static final String HCAT_KEY_TOKEN_SIGNATURE = HCAT_KEY_OUTPUT_BASE + ".token.sig";
}
