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

package org.apache.hcatalog.data.transfer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.transfer.state.StateProvider;

/**
 * This abstract class is internal to HCatalog and abstracts away the notion of
 * underlying system from which reads will be done.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.transfer.HCatReader} instead
 */

public abstract class HCatReader {

  /**
   * This should be called at master node to obtain {@link ReaderContext} which
   * then should be serialized and sent to slave nodes.
   *
   * @return {@link ReaderContext}
   * @throws HCatException
   */
  public abstract ReaderContext prepareRead() throws HCatException;

  /**
   * This should be called at slave nodes to read {@link HCatRecord}s
   *
   * @return {@link Iterator} of {@link HCatRecord}
   * @throws HCatException
   */
  public abstract Iterator<HCatRecord> read() throws HCatException;

  /**
   * This constructor will be invoked by {@link DataTransferFactory} at master
   * node. Don't use this constructor. Instead, use {@link DataTransferFactory}
   *
   * @param re
   * @param config
   */
  protected HCatReader(final ReadEntity re, final Map<String, String> config) {
    this(config);
    this.re = re;
  }

  /**
   * This constructor will be invoked by {@link DataTransferFactory} at slave
   * nodes. Don't use this constructor. Instead, use {@link DataTransferFactory}
   *
   * @param config
   * @param sp
   */

  protected HCatReader(final Configuration config, StateProvider sp) {
    this.conf = config;
    this.sp = sp;
  }

  protected ReadEntity re; // This will be null at slaves.
  protected Configuration conf;
  protected ReaderContext info;
  protected StateProvider sp; // This will be null at master.

  private HCatReader(final Map<String, String> config) {
    Configuration conf = new Configuration();
    if (null != config) {
      for (Entry<String, String> kv : config.entrySet()) {
        conf.set(kv.getKey(), kv.getValue());
      }
    }
    this.conf = conf;
  }

  public Configuration getConf() {
    if (null == conf) {
      throw new IllegalStateException(
        "HCatReader is not constructed correctly.");
    }
    return conf;
  }
}
