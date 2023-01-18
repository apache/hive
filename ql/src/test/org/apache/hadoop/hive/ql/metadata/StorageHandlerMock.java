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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Mock class used for unit testing.
 * {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2#testLockingOnInsertIntoNonNativeTables()}
 */
public class StorageHandlerMock extends DefaultStorageHandler {
  @Override public HiveMetaHook getMetaHook() {
    return new HiveMetaHook() {
      @Override public void preCreateTable(Table table) throws MetaException {

      }

      @Override public void rollbackCreateTable(Table table) throws MetaException {

      }

      @Override public void commitCreateTable(Table table) throws MetaException {

      }

      @Override public void preDropTable(Table table) throws MetaException {

      }

      @Override public void rollbackDropTable(Table table) throws MetaException {

      }

      @Override public void commitDropTable(Table table, boolean deleteData) throws MetaException {

      }
    };
  }

  @Override public LockType getLockType(WriteEntity writeEntity) {
    if (writeEntity.getWriteType().equals(WriteEntity.WriteType.INSERT)) {
      return LockType.SHARED_READ;
    }
    return super.getLockType(writeEntity);
  }

  @Override public Class<? extends OutputFormat> getOutputFormatClass() {
    return MockOutputFormat.class;
  }

  /**
   * Dummy no op output format.
   */
  public static class MockOutputFormat implements OutputFormat {

    @Override public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
        Progressable progressable
    ) throws IOException {
      return new RecordWriter() {
        @Override public void write(Object o, Object o2) throws IOException {
          //noop
        }

        @Override public void close(Reporter reporter) throws IOException {

        }
      };
    }

    @Override public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
  }
}
