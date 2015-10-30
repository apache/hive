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
package org.apache.hive.hcatalog.templeton.tool;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *  HDFS implementation of templeton storage.
 *
 *  This implementation assumes that all keys in key/value pairs are
 *  chosen such that they don't have any newlines in them.
 *
 */
public class HDFSStorage implements TempletonStorage {
  FileSystem fs = null;

  public String storage_root = null;

  public static final String JOB_PATH = "/jobs";
  public static final String JOB_TRACKINGPATH = "/created";
  public static final String OVERHEAD_PATH = "/overhead";

  private static final Logger LOG = LoggerFactory.getLogger(HDFSStorage.class);

  public void startCleanup(Configuration config) {
    try {
      HDFSCleanup.startInstance(config);
    } catch (Exception e) {
      LOG.warn("Cleanup instance didn't start.");
    }
  }

  @Override
  public void saveField(Type type, String id, String key, String val)
    throws NotFoundException {
    if (val == null) {
      return;
    }
    PrintWriter out = null;
    //todo: FileSystem#setPermission() - should this make sure to set 777 on jobs/ ?
    Path keyfile= new Path(getPath(type) + "/" + id + "/" + key); 
    try {
      // This will replace the old value if there is one
      // Overwrite the existing file
      out = new PrintWriter(new OutputStreamWriter(fs.create(keyfile)));
      out.write(val);
      out.flush();
    } catch (Exception e) {
      String errMsg = "Couldn't write to " + keyfile + ": " + e.getMessage();
      LOG.error(errMsg, e);
      throw new NotFoundException(errMsg, e);
    } finally {
      close(out);
    }
  }

  @Override
  public String getField(Type type, String id, String key) {
    BufferedReader in = null;
    Path p = new Path(getPath(type) + "/" + id + "/" + key);
    try {
      if(!fs.exists(p)) {
        //check first, otherwise webhcat.log is full of stack traces from FileSystem when
        //clients check for status ('exitValue', 'completed', etc.)
        LOG.debug(p + " does not exist.");
        return null;
      }
      in = new BufferedReader(new InputStreamReader(fs.open(p)));
      String line = null;
      String val = "";
      while ((line = in.readLine()) != null) {
        if (!val.equals("")) {
          val += "\n";
        }
        val += line;
      }
      return val;
    } catch (Exception e) {
      LOG.error("Couldn't find " + p + ": " + e.getMessage(), e);
    } finally {
      close(in);
    }
    return null;
  }

  @Override
  public boolean delete(Type type, String id) throws NotFoundException {
    Path p = new Path(getPath(type) + "/" + id);
    try {
      fs.delete(p, true);
    } catch (IOException e) {
      throw new NotFoundException("Node " + p + " was not found: " +
        e.getMessage());
    }
    return false;
  }


  @Override
  public List<String> getAllForType(Type type) {
    ArrayList<String> allNodes = new ArrayList<String>();
    try {
      for (FileStatus status : fs.listStatus(new Path(getPath(type)))) {
        allNodes.add(status.getPath().getName());
      }
      return null;
    } catch (Exception e) {
      LOG.trace("Couldn't find children for type " + type.toString());
    }
    return allNodes;
  }

  @Override
  public void openStorage(Configuration config) throws IOException {
    storage_root = config.get(TempletonStorage.STORAGE_ROOT);
    if (fs == null) {
      fs = new Path(storage_root).getFileSystem(config);
    }
  }

  @Override
  public void closeStorage() throws IOException {
    // Nothing to do here
  }

  /**
   * Get the path to storage based on the type.
   * @param type
   */
  public String getPath(Type type) {
    return getPath(type, storage_root);
  }

  /**
   * Static method to get the path based on the type.
   *
   * @param type
   * @param root
   */
  public static String getPath(Type type, String root) {
    String typepath = root + OVERHEAD_PATH;
    switch (type) {
    case JOB:
      typepath = root + JOB_PATH;
      break;
    case JOBTRACKING:
      typepath = root + JOB_TRACKINGPATH;
      break;
    }
    return typepath;
  }
  private void close(Closeable is) {
    if(is == null) {
      return;
    }
    try {
      is.close();
    }
    catch (IOException ex) {
      LOG.trace("Failed to close InputStream: " + ex.getMessage());
    }
  }
}
