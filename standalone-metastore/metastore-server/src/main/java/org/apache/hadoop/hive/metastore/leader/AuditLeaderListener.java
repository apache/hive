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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
          .addCol("leader", ColumnType.STRING_TYPE_NAME)
          .addCol("type", ColumnType.STRING_TYPE_NAME)
          .addCol("elected_time", ColumnType.STRING_TYPE_NAME)
          .setOwner(SecurityUtils.getUser())
          .setOwnerType(PrincipalType.USER)
          .setSerdeLib(SERDE)
          .setInputFormat(INPUTFORMAT)
          .setOutputFormat(OUTPUTFORMAT)
          .build(handler.getConf());
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
      throw new RuntimeException(tableName + " should be in json + text format");
    }

  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    String hostName = getHostname();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String message = "{\"leader\": \"" + hostName + "\", \"type\": \""
        + election.getName() + "\", \"elected_time\": \"" + LocalDateTime.now().format(formatter) + "\"} \n";
    Path path = new Path(tableLocation, "leader_" + election.getName() + ".json");
    try {
      FileSystem fs = Warehouse.getFs(path, configuration);
      try (OutputStream outputStream = fs.exists(path) ?
          fs.append(path) :
          fs.create(path, false)) {
        outputStream.write(message.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      }
    } catch (Exception e) {
      HiveMetaStore.LOG.error("Error while writing the leader info, path: " + path, e);
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
    // do nothing
  }

}
