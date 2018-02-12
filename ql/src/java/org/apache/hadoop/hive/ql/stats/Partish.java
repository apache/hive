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

package org.apache.hadoop.hive.ql.stats;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * Cover class to make it easier to make modifications on partitions/tables
 */
public abstract class Partish {

  public static Partish buildFor(Table table) {
    return new PTable(table);
  }

  public static Partish buildFor(Partition part) {
    return new PPart(part.getTable(), part);
  }

  public static Partish buildFor(Table table, Partition part) {
    return new PPart(table, part);
  }

  // rename
  @Deprecated
  public final boolean isAcid() {
    return AcidUtils.isFullAcidTable(getTable());
  }

  public abstract Table getTable();

  public abstract Map<String, String> getPartParameters();

  public abstract StorageDescriptor getPartSd();

  public abstract Object getOutput() throws HiveException;

  public abstract Partition getPartition();

  public abstract Class<? extends InputFormat> getInputFormatClass() throws HiveException;

  public abstract Class<? extends OutputFormat> getOutputFormatClass() throws HiveException;

  public abstract String getLocation();

  public abstract String getSimpleName();

  public final String getPartishType() {
    return getClass().getSimpleName();
  }

  static class PTable extends Partish {
    private Table table;

    public PTable(Table table) {
      this.table = table;
    }

    @Override
    public Table getTable() {
      return table;
    }

    @Override
    public Map<String, String> getPartParameters() {
      return table.getTTable().getParameters();
    }

    @Override
    public StorageDescriptor getPartSd() {
      return table.getTTable().getSd();
    }

    @Override
    public Object getOutput() throws HiveException {
      return new Table(getTable().getTTable());
    }

    @Override
    public Partition getPartition() {
      return null;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
      return table.getInputFormatClass();
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
      return table.getOutputFormatClass();
    }

    @Override
    public String getLocation() {
      return table.getDataLocation().toString();
    }

    @Override
    public String getSimpleName() {
      return String.format("Table %s.%s", table.getDbName(), table.getTableName());
    }
  }

  static class PPart extends Partish {
    private Table table;
    private Partition partition;

    // FIXME: possibly the distinction between table/partition is not need; however it was like this before....will change it later
    public PPart(Table table, Partition partiton) {
      this.table = table;
      partition = partiton;
    }

    @Override
    public Table getTable() {
      return table;
    }

    @Override
    public Map<String, String> getPartParameters() {
      return partition.getTPartition().getParameters();
    }

    @Override
    public StorageDescriptor getPartSd() {
      return partition.getTPartition().getSd();
    }

    @Override
    public Object getOutput() throws HiveException {
      return new Partition(table, partition.getTPartition());
    }

    @Override
    public Partition getPartition() {
      return partition;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() throws HiveException {
      return partition.getInputFormatClass();
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() throws HiveException {
      return partition.getOutputFormatClass();
    }

    @Override
    public String getLocation() {
      return partition.getLocation();
    }

    @Override
    public String getSimpleName() {
      return String.format("Partition %s.%s %s", table.getDbName(), table.getTableName(), partition.getSpec());
    }

  }

}