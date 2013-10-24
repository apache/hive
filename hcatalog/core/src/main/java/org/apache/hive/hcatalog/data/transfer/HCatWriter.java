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

package org.apache.hive.hcatalog.data.transfer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.state.StateProvider;

/**
 * This abstraction is internal to HCatalog. This is to facilitate writing to
 * HCatalog from external systems. Don't try to instantiate this directly.
 * Instead, use {@link DataTransferFactory}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HCatWriter {

  protected Configuration conf;
  protected WriteEntity we; // This will be null at slave nodes.
  protected WriterContext info;
  protected StateProvider sp;

  /**
   * External system should invoke this method exactly once from a master node.
   *
   * @return {@link WriterContext} This should be serialized and sent to slave
   *         nodes to construct HCatWriter there.
   * @throws HCatException
   */
  public abstract WriterContext prepareWrite() throws HCatException;

  /**
   * This method should be used at slave needs to perform writes.
   *
   * @param recordItr
   *          {@link Iterator} records to be written into HCatalog.
   * @throws {@link HCatException}
   */
  public abstract void write(final Iterator<HCatRecord> recordItr)
    throws HCatException;

  /**
   * This method should be called at master node. Primary purpose of this is to
   * do metadata commit.
   *
   * @throws {@link HCatException}
   */
  public abstract void commit(final WriterContext context) throws HCatException;

  /**
   * This method should be called at master node. Primary purpose of this is to
   * do cleanups in case of failures.
   *
   * @throws {@link HCatException} *
   */
  public abstract void abort(final WriterContext context) throws HCatException;

  /**
   * This constructor will be used at master node
   *
   * @param we
   *          WriteEntity defines where in storage records should be written to.
   * @param config
   *          Any configuration which external system wants to communicate to
   *          HCatalog for performing writes.
   */
  protected HCatWriter(final WriteEntity we, final Map<String, String> config) {
    this(config);
    this.we = we;
  }

  /**
   * This constructor will be used at slave nodes.
   *
   * @param config
   */
  protected HCatWriter(final Configuration config, final StateProvider sp) {
    this.conf = config;
    this.sp = sp;
  }

  private HCatWriter(final Map<String, String> config) {
    Configuration conf = new Configuration();
    if (config != null) {
      // user is providing config, so it could be null.
      for (Entry<String, String> kv : config.entrySet()) {
        conf.set(kv.getKey(), kv.getValue());
      }
    }

    this.conf = conf;
  }
}
