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

package org.apache.hadoop.hive.ql.metadata;

import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * HiveStorageHandler defines a pluggable interface for adding
 * new storage handlers to Hive.  A storage handler consists of
 * a bundle of the following:
 *
 *<ul>
 *<li>input format
 *<li>output format
 *<li>serde
 *<li>metadata hooks for keeping an external catalog in sync
 * with Hive's metastore
 *<li>rules for setting up the configuration properties on
 * map/reduce jobs which access tables stored by this handler
 *</ul>
 *
 * Storage handler classes are plugged in using the STORED BY 'classname'
 * clause in CREATE TABLE.
 */
public interface HiveStorageHandler extends Configurable {
  /**
   * @return Class providing an implementation of {@link InputFormat}
   */
  public Class<? extends InputFormat> getInputFormatClass();

  /**
   * @return Class providing an implementation of {@link OutputFormat}
   */
  public Class<? extends OutputFormat> getOutputFormatClass();

  /**
   * @return Class providing an implementation of {@link SerDe}
   */
  public Class<? extends SerDe> getSerDeClass();

  /**
   * @return metadata hook implementation, or null if this
   * storage handler does not need any metadata notifications
   */
  public HiveMetaHook getMetaHook();

  /**
   * Configures properties for a job based on the definition of the
   * source or target table it accesses.
   *
   * @param tableDesc descriptor for the table being accessed
   *
   * @param jobProperties receives properties copied or transformed
   * from the table properties
   */
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties);
}
