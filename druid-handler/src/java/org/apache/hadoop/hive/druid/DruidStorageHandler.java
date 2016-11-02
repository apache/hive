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
package org.apache.hadoop.hive.druid;

import com.google.common.base.Throwables;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.DruidOutputFormatUtils;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({"deprecation","rawtypes"})
public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveDruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveDruidOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Do safety checks
    if (MetaStoreUtils.isExternalTable(table) && !StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid existing sources");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Nothing to do
    final Path segmentDescriptorDir = new Path(table.getSd().getLocation());
    try {
      List<DataSegment> dataSegmentList = DruidOutputFormatUtils.getPublishedSegments(segmentDescriptorDir, getConf());
      for (DataSegment dataSegment :
          dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        }
        catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while trying to clean the segment [%s]", dataSegment), e);
        }
      }
    }
    catch (IOException e) {
      LOG.error("Exception while rollback", e);
      Throwables.propagate(e);
    }

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    //@TODO // FIXME: 10/31/16 get the actual outPutPath
    final Path segmentDescriptorDir = new Path(table.getSd().getLocation());
    try {
      publishSegments(DruidOutputFormatUtils.getPublishedSegments(segmentDescriptorDir, getConf()));
    }
    catch (IOException e) {
      LOG.error("Exception while commit", e);
      Throwables.propagate(e);
    }
  }

  public void deleteSegment(DataSegment segment) throws SegmentLoadingException
  {

    final Path path = getPath(segment);
    LOG.info("removing segment[%s] mapped to path[%s]", segment.getIdentifier(), path);

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(getConf());

        if (!fs.exists(path)) {
          LOG.warn("Segment Path [%s] does not exist. It appears to have been deleted already.", path);
          return ;
        }

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        Path partitionNumDir = path.getParent();
        if (!fs.delete(partitionNumDir, true)) {
          throw new SegmentLoadingException(
              "Unable to kill segment, failed to delete dir [%s]",
              partitionNumDir.toString()
          );
        }

        //try to delete other directories if possible
        Path versionDir = partitionNumDir.getParent();
        if (safeNonRecursiveDelete(fs, versionDir)) {
          Path intervalDir = versionDir.getParent();
          if (safeNonRecursiveDelete(fs, intervalDir)) {
            Path dataSourceDir = intervalDir.getParent();
            safeNonRecursiveDelete(fs, dataSourceDir);
          }
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  private Path getPath(DataSegment dataSegment)
  {
    return new Path(String.valueOf(dataSegment.getLoadSpec().get("path")));
  }

  private boolean safeNonRecursiveDelete(FileSystem fs, Path path)
  {
    try {
      return fs.delete(path, false);
    }
    catch (Exception ex) {
      return false;
    }
  }

  private void publishSegments(List<DataSegment> publishedSegments)
  {
    //insert into db
    return;
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    // Nothing to do
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

}
