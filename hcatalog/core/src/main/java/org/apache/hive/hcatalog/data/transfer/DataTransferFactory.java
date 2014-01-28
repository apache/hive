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

import java.util.Map;

import org.apache.hive.hcatalog.data.transfer.impl.HCatInputFormatReader;
import org.apache.hive.hcatalog.data.transfer.impl.HCatOutputFormatWriter;
import org.apache.hive.hcatalog.data.transfer.state.DefaultStateProvider;
import org.apache.hive.hcatalog.data.transfer.state.StateProvider;

/**
 * Use this factory to get instances of {@link HCatReader} or {@link HCatWriter}
 * at master and slave nodes.
 */

public class DataTransferFactory {

  /**
   * This should be called once from master node to obtain an instance of
   * {@link HCatReader}.
   *
   * @param re
   *          ReadEntity built using {@link ReadEntity.Builder}
   * @param config
   *          any configuration which master node wants to pass to HCatalog
   * @return {@link HCatReader}
   */
  public static HCatReader getHCatReader(final ReadEntity re,
                       final Map<String, String> config) {
    // In future, this may examine ReadEntity and/or config to return
    // appropriate HCatReader
    return new HCatInputFormatReader(re, config);
  }

  /**
   * This should only be called once from every slave node to obtain an instance
   * of {@link HCatReader}.
   *
   * @param context
   *          reader context obtained at the master node
   * @param slaveNumber
   *          which slave this is, determines which part of the read is done
   * @return {@link HCatReader}
   */
  public static HCatReader getHCatReader(final ReaderContext context,
                                         int slaveNumber) {
    // In future, this may examine config to return appropriate HCatReader
    return getHCatReader(context, slaveNumber, DefaultStateProvider.get());
  }

  /**
   * This should only be called once from every slave node to obtain an instance
   * of {@link HCatReader}. This should be called if an external system has some
   * state to provide to HCatalog.
   *
   * @param context
   *          reader context obtained at the master node
   * @param slaveNumber
   *          which slave this is, determines which part of the read is done
   * @param sp
   *          {@link StateProvider}
   * @return {@link HCatReader}
   */
  public static HCatReader getHCatReader(final ReaderContext context,
                                         int slaveNumber,
                                         StateProvider sp) {
    // In future, this may examine config to return appropriate HCatReader
    return new HCatInputFormatReader(context, slaveNumber, sp);
  }

  /**
   * This should be called at master node to obtain an instance of
   * {@link HCatWriter}.
   *
   * @param we
   *          WriteEntity built using {@link WriteEntity.Builder}
   * @param config
   *          any configuration which master wants to pass to HCatalog
   * @return {@link HCatWriter}
   */
  public static HCatWriter getHCatWriter(final WriteEntity we,
                       final Map<String, String> config) {
    // In future, this may examine WriteEntity and/or config to return
    // appropriate HCatWriter
    return new HCatOutputFormatWriter(we, config);
  }

  /**
   * This should be called at slave nodes to obtain an instance of
   * {@link HCatWriter}.
   *
   * @param cntxt
   *          {@link WriterContext} obtained at master node
   * @return {@link HCatWriter}
   */
  public static HCatWriter getHCatWriter(final WriterContext cntxt) {
    // In future, this may examine context to return appropriate HCatWriter
    return getHCatWriter(cntxt, DefaultStateProvider.get());
  }

  /**
   * This should be called at slave nodes to obtain an instance of
   * {@link HCatWriter}. If an external system has some mechanism for providing
   * state to HCatalog, this constructor can be used.
   *
   * @param cntxt
   *          {@link WriterContext} obtained at master node
   * @param sp
   *          {@link StateProvider}
   * @return {@link HCatWriter}
   */
  public static HCatWriter getHCatWriter(final WriterContext cntxt,
                       final StateProvider sp) {
    // In future, this may examine context to return appropriate HCatWriter
    return new HCatOutputFormatWriter(cntxt, sp);
  }
}
