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
package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * This class listens for drop events and, if set, exports the table's metadata as JSON to the trash
 * of the user performing the drop
 */
public class MetaDataExportListener extends MetaStorePreEventListener {
  public static final Logger LOG = LoggerFactory.getLogger(MetaDataExportListener.class);

  /** Configure the export listener */
  public MetaDataExportListener(Configuration config) {
    super(config);
  }

  /** Export the metadata to a given path, and then move it to the user's trash */
  private void export_meta_data(PreDropTableEvent tableEvent) throws MetaException {
    FileSystem fs = null;
    Table tbl = tableEvent.getTable();
    String name = tbl.getTableName();
    org.apache.hadoop.hive.ql.metadata.Table mTbl = new org.apache.hadoop.hive.ql.metadata.Table(
        tbl);
    IHMSHandler handler = tableEvent.getHandler();
    Configuration conf = handler.getConf();
    Warehouse wh = new Warehouse(conf);
    Path tblPath = new Path(tbl.getSd().getLocation());
    fs = wh.getFs(tblPath);
    Date now = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    String dateString = sdf.format(now);
    String exportPathString = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METADATA_EXPORT_LOCATION);
    boolean moveMetadataToTrash = MetastoreConf
        .getBoolVar(conf, MetastoreConf.ConfVars.MOVE_EXPORTED_METADATA_TO_TRASH);
    Path exportPath = null;
    if (exportPathString != null && exportPathString.length() == 0) {
      exportPath = fs.getHomeDirectory();
    } else {
      exportPath = new Path(exportPathString);
    }
    Path metaPath = new Path(exportPath, name + "." + dateString);
    LOG.info("Exporting the metadata of table " + tbl.toString() + " to path "
        + metaPath.toString());
    try {
      fs.mkdirs(metaPath);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    Path outFile = new Path(metaPath, name + EximUtil.METADATA_NAME);
    try {
      SessionState.getConsole().printInfo("Beginning metadata export");
      EximUtil.createExportDump(fs, outFile, mTbl, null, null,
          new HiveConf(conf, MetaDataExportListener.class));
      if (moveMetadataToTrash == true) {
        wh.deleteDir(metaPath, true, false, false);
      }
    } catch (IOException | SemanticException e) {
      throw new MetaException(e.getMessage());
    }
  }

  /**
   * Listen for an event; if it is a DROP_TABLE event, call export_meta_data
   * */
  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {
    if (context.getEventType() == PreEventType.DROP_TABLE) {
      export_meta_data((PreDropTableEvent) context);
    }
  }

}
