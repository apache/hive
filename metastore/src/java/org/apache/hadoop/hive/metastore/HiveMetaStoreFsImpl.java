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

package org.apache.hadoop.hive.metastore;

import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class HiveMetaStoreFsImpl implements MetaStoreFS {

  public static final Log LOG = LogFactory
      .getLog("hive.metastore.hivemetastoressimpl");

  @Override
  public boolean deleteDir(FileSystem fs, Path f, boolean recursive,
      Configuration conf) throws MetaException {
    LOG.info("deleting  " + f);

    // older versions of Hadoop don't have a Trash constructor based on the
    // Path or FileSystem. So need to achieve this by creating a dummy conf.
    // this needs to be filtered out based on version
    Configuration dupConf = new Configuration(conf);
    FileSystem.setDefaultUri(dupConf, fs.getUri());

    try {
      Trash trashTmp = new Trash(dupConf);
      if (trashTmp.moveToTrash(f)) {
        LOG.info("Moved to trash: " + f);
        return true;
      }

      if (fs.delete(f, true)) {
        LOG.info("Deleted the diretory " + f);
        return true;
      }

      if (fs.exists(f)) {
        throw new MetaException("Unable to delete directory: " + f);
      }
    } catch (FileNotFoundException e) {
      return true; // ok even if there is not data
    } catch (Exception e) {
      Warehouse.closeFs(fs);
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

}
