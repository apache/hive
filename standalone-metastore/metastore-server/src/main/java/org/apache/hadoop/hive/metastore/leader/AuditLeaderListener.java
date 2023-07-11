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

package org.apache.hadoop.hive.metastore.leader;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AuditLeaderListener implements LeaderElection.LeadershipStateListener {
  private final Configuration configuration;
  private final Path tableLocation;

  private final static String SERDE = "org.apache.hadoop.hive.serde2.JsonSerDe";
  private final static String INPUTFORMAT = "org.apache.hadoop.mapred.TextInputFormat";
  private final static String OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat";

  public AuditLeaderListener(TableName tableName, IHMSHandler handler) throws Exception {
    requireNonNull(tableName, "tableName is null");
    requireNonNull(handler, "handler is null");
    this.configuration = handler.getConf();
    try {
      // Store the leader info as json + text for human-readable
      Table table = new TableBuilder()
          .setCatName(tableName.getCat())
          .setDbName(tableName.getDb())
          .setTableName(tableName.getTable())
          .addCol("leader_host", ColumnType.STRING_TYPE_NAME)
          .addCol("leader_type", ColumnType.STRING_TYPE_NAME)
          .addCol("elected_time", ColumnType.STRING_TYPE_NAME)
          .setOwner(SecurityUtils.getUser())
          .setOwnerType(PrincipalType.USER)
          .setSerdeLib(SERDE)
          .setInputFormat(INPUTFORMAT)
          .setOutputFormat(OUTPUTFORMAT)
          .addTableParam("EXTERNAL", "TRUE")
          .addTableParam(MetaStoreUtils.EXTERNAL_TABLE_PURGE, "TRUE")
          .build(handler.getConf());
      table.setTableType(TableType.EXTERNAL_TABLE.toString());
      handler.create_table(table);
    } catch (AlreadyExistsException e) {
      // ignore
    }

    Table table = handler.getMS().getTable(tableName.getCat(),
        tableName.getDb(), tableName.getTable());
    this.tableLocation = new Path(table.getSd().getLocation());
    String serde = table.getSd().getSerdeInfo().getSerializationLib();
    String input = table.getSd().getInputFormat();
    String output = table.getSd().getOutputFormat();
    if (!SERDE.equals(serde) || !INPUTFORMAT.equals(input)
        || !OUTPUTFORMAT.equals(output)) {
      throw new RuntimeException(tableName + " should be a plain json table");
    }
  }

  // For testing purpose only
  @VisibleForTesting
  public AuditLeaderListener(Path tableLocation, Configuration configuration) {
    this.tableLocation = tableLocation;
    this.configuration = configuration;
  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    HiveMetaStore.LOG.info("Became the LEADER for {}", election.getName());
    String hostName = getHostname();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String message = "{\"leader_host\": \"" + hostName + "\", \"leader_type\": \""
        + election.getName() + "\", \"elected_time\": \"" + LocalDateTime.now().format(formatter) + "\"} \n";

    Path path = null;
    try {
      FileSystem fs = Warehouse.getFs(tableLocation, configuration);
      boolean createNewFile = FileUtils.isS3a(fs) || MetastoreConf.getBoolVar(configuration,
          MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_NEW_AUDIT_FILE);
      String prefix =  "leader_" + election.getName();
      String fileName = createNewFile ?
          prefix + "_" + System.currentTimeMillis() + ".json" :
          prefix + ".json";
      path = new Path(tableLocation, fileName);
      // Audit the election event
      try (OutputStream outputStream = createNewFile ?
          fs.create(path, false) :
          (fs.exists(path) ? fs.append(path) : fs.create(path, false))) {
        outputStream.write(message.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      }
      // Trash out small files when the file system cannot support append
      final int limit = MetastoreConf.getIntVar(configuration,
          MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_AUDIT_FILE_LIMIT);
      if (createNewFile && limit > 0) {
        List<FileStatus> allFileStatuses = FileUtils.getFileStatusRecurse(tableLocation, fs);
        List<FileStatus> thisLeaderFiles = new ArrayList<>();
        // All this leader audit file must start with leader_$name_
        allFileStatuses.stream().forEach(f -> {
          if (f.getPath().getName().startsWith(prefix)) {
            thisLeaderFiles.add(f);
          }
        });
        if (thisLeaderFiles.size() > limit) {
          thisLeaderFiles.sort((Comparator.comparing(o -> o.getPath().getName())));
          // delete the files that beyond the limit
          for (int i = 0; i < thisLeaderFiles.size() - limit; i++) {
            FileUtils.moveToTrash(fs, thisLeaderFiles.get(i).getPath(), configuration, true);
          }
        }
      }
    } catch (Exception e) {
      HiveMetaStore.LOG.error("Error while writing the leader info into file: " + path, e);
    }
  }

  // copy from HiveMetaStore
  private static String getHostname() {
    try {
      return "" + InetAddress.getLocalHost();
    } catch(UnknownHostException uhe) {
      return "" + uhe;
    }
  }

  @Override
  public void lossLeadership(LeaderElection election) throws Exception {
    HiveMetaStore.LOG.info("Lost leadership for {}", election.getName());
  }

}
