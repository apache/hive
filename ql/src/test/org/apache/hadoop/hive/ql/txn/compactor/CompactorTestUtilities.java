/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CompactorTestUtilities {
  private static final Logger LOG = LoggerFactory.getLogger(CompactorTestUtilities.class);

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final int ORC_ACID_VERSION_DEFAULT = 0;
  public enum CompactorThreadType {INITIATOR, WORKER, CLEANER}

  /**
   * This is smart enough to handle streaming ingest where there could be a
   * {@link AcidUtils#DELTA_SIDE_FILE_SUFFIX} side file.
   * @param dataFile - ORC acid data file
   * @return version property from file if there,
   *          {@link #ORC_ACID_VERSION_DEFAULT} otherwise
   */
  private static int getAcidVersionFromDataFile(Path dataFile, FileSystem fs) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(dataFile);
    try (Reader orcReader = OrcFile.createReader(dataFile,
        OrcFile.readerOptions(fs.getConf())
            .filesystem(fs)
            //make sure to check for side file in case streaming ingest died
            .maxLength(AcidUtils.getLogicalLength(fs, fileStatus)))) {
      if (orcReader.hasMetadataValue(AcidUtils.OrcAcidVersion.ACID_VERSION_KEY)) {
        char[] versionChar =
            UTF8.decode(orcReader.getMetadataValue(AcidUtils.OrcAcidVersion.ACID_VERSION_KEY)).array();
        String version = new String(versionChar);
        return Integer.valueOf(version);
      }
    }
    return ORC_ACID_VERSION_DEFAULT;
  }

  private static int getAcidVersionFromMetaFile(Path deltaOrBaseDir, FileSystem fs)
      throws IOException {
    Path formatFile = AcidUtils.OrcAcidVersion.getVersionFilePath(deltaOrBaseDir);
    try (FSDataInputStream inputStream = fs.open(formatFile)) {
      byte[] bytes = new byte[1];
      int read = inputStream.read(bytes);
      if (read != -1) {
        String version = new String(bytes, UTF8);
        return Integer.valueOf(version);
      }
      return ORC_ACID_VERSION_DEFAULT;
    } catch (FileNotFoundException fnf) {
      LOG.debug(formatFile + " not found, returning default: " + ORC_ACID_VERSION_DEFAULT);
      return ORC_ACID_VERSION_DEFAULT;
    } catch(IOException ex) {
      LOG.error(formatFile + " is unreadable due to: " + ex.getMessage(), ex);
      throw ex;
    }
  }

  public static void checkAcidVersion(RemoteIterator<LocatedFileStatus> files, FileSystem fs, boolean hasVersionFile,
      String[] expectedTypes) throws IOException
  {
    Set<String> foundPrefixes = new HashSet<>(expectedTypes.length);
    Set<String> foundDirectories = new HashSet<>(expectedTypes.length);
    while(files.hasNext()) {
      LocatedFileStatus file = files.next();
      Path path = file.getPath();
      if (!path.getName().equals(AcidUtils.OrcAcidVersion.ACID_FORMAT)) {
        int version = getAcidVersionFromDataFile(path, fs);
        //check that files produced by compaction still have the version marker
        Assert.assertEquals("Unexpected version marker in " + path.getName(),
            AcidUtils.OrcAcidVersion.ORC_ACID_VERSION, version);
      }
      Path parent = path.getParent();
      if (!foundDirectories.contains(parent)) {
        if (parent.getName().startsWith(AcidUtils.BASE_PREFIX)) {
          foundPrefixes.add(AcidUtils.BASE_PREFIX);
        } else if (parent.getName().startsWith(AcidUtils.DELTA_PREFIX)) {
          foundPrefixes.add(AcidUtils.DELTA_PREFIX);
        } else if (parent.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)) {
          foundPrefixes.add(AcidUtils.DELETE_DELTA_PREFIX);
        }
        // It is a directory
        if (hasVersionFile) {
          Assert.assertTrue("Version marker should exists",
              fs.exists(AcidUtils.OrcAcidVersion.getVersionFilePath(parent)));
          int versionFromMetaFile = getAcidVersionFromMetaFile(parent, fs);
          Assert.assertEquals("Unexpected version marker in " + parent,
              AcidUtils.OrcAcidVersion.ORC_ACID_VERSION, versionFromMetaFile);
        } else {
          Assert.assertFalse("Version marker should not exists",
              fs.exists(AcidUtils.OrcAcidVersion.getVersionFilePath(parent)));
        }
      }
    }
    Assert.assertEquals("Did not found all types of directories", new HashSet<>(Arrays.asList(expectedTypes)),
        foundPrefixes);
  }
}
