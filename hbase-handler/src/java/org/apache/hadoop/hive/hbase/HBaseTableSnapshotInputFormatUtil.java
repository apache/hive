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

package org.apache.hadoop.hive.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A helper class to isolate newer HBase features from users running against older versions of
 * HBase that don't provide those features.
 *
 * TODO: remove this class when it's okay to drop support for earlier version of HBase.
 */
public class HBaseTableSnapshotInputFormatUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableSnapshotInputFormatUtil.class);

  /** The class we look for to determine if hbase snapshots are supported. */
  private static final String TABLESNAPSHOTINPUTFORMAT_CLASS
    = "org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl";

  private static final String TABLESNAPSHOTREGIONSPLIT_CLASS
    = "org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat$TableSnapshotRegionSplit";

  /** True when {@link #TABLESNAPSHOTINPUTFORMAT_CLASS} is present. */
  private static final boolean SUPPORTS_TABLE_SNAPSHOTS;

  static {
    boolean support = false;
    try {
      Class<?> clazz = Class.forName(TABLESNAPSHOTINPUTFORMAT_CLASS);
      support = clazz != null;
    } catch (ClassNotFoundException e) {
      // pass
    }
    SUPPORTS_TABLE_SNAPSHOTS = support;
  }

  /** Return true when the HBase runtime supports {@link HiveHBaseTableSnapshotInputFormat}. */
  public static void assertSupportsTableSnapshots() {
    if (!SUPPORTS_TABLE_SNAPSHOTS) {
      throw new RuntimeException("This version of HBase does not support Hive over table " +
        "snapshots. Please upgrade to at least HBase 0.98.3 or later. See HIVE-6584 for details.");
    }
  }

  /**
   * Configures {@code conf} for the snapshot job. Call only when
   * {@link #assertSupportsTableSnapshots()} returns true.
   */
  public static void configureJob(Configuration conf, String snapshotName, Path restoreDir)
      throws IOException {
    TableSnapshotInputFormatImpl.setInput(conf, snapshotName, restoreDir);
  }

  /**
   * Create a bare TableSnapshotRegionSplit. Needed because Writables require a
   * default-constructed instance to hydrate from the DataInput.
   *
   * TODO: remove once HBASE-11555 is fixed.
   */
  public static InputSplit createTableSnapshotRegionSplit() {
    try {
      assertSupportsTableSnapshots();
    } catch (RuntimeException e) {
      LOG.debug("Probably don't support table snapshots. Returning null instance.", e);
      return null;
    }

    try {
      Class<? extends InputSplit> resultType =
        (Class<? extends InputSplit>) Class.forName(TABLESNAPSHOTREGIONSPLIT_CLASS);
      Constructor<? extends InputSplit> cxtor = resultType.getDeclaredConstructor(new Class[]{});
      cxtor.setAccessible(true);
      return cxtor.newInstance(new Object[]{});
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
        "Unable to find " + TABLESNAPSHOTREGIONSPLIT_CLASS, e);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
        "Unable to access specified class " + TABLESNAPSHOTREGIONSPLIT_CLASS, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
        "Unable to instantiate specified class " + TABLESNAPSHOTREGIONSPLIT_CLASS, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
        "Constructor threw an exception for " + TABLESNAPSHOTREGIONSPLIT_CLASS, e);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
        "Unable to find suitable constructor for class " + TABLESNAPSHOTREGIONSPLIT_CLASS, e);
    }
  }
}
