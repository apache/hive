/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestMsckCheckPartitions {

  /**
   * Test counts the number of listStatus calls in the msck core method of
   * listing sub-directories. This is important to check since it unnecessary
   * listStatus calls could cause performance degradation in remote filesystems
   * like S3. The test creates a mock FileSystem object and a mock directory structure
   * to simulate a table which has 2 partition keys and 2 partition values at each level.
   * In the end it counts how many times the listStatus is called on the mock filesystem
   * and confirm its equal to the current theoretical value.
   *
   * @throws IOException
   * @throws MetastoreException
   */
  @Test
  public void testNumberOfListStatusCalls() throws IOException, MetastoreException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    Path countryUS = new Path(tableLocation, "country=US");
    Path countryIND = new Path(tableLocation, "country=IND");

    Path cityPA = new Path(countryUS, "city=PA");
    Path citySF = new Path(countryUS, "city=SF");
    Path cityBOM = new Path(countryIND, "city=BOM");
    Path cityDEL = new Path(countryIND, "city=DEL");

    Path paData = new Path(cityPA, "datafile");
    Path sfData = new Path(citySF, "datafile");
    Path bomData = new Path(cityBOM, "datafile");
    Path delData = new Path(cityDEL, "datafile");

    //level 1 listing
    FileStatus[] allCountries = getMockFileStatus(countryUS, countryIND);
    mockListStatusIterator(mockFs, tableLocation, allCountries);

    //level 2 listing
    FileStatus[] filesInUS = getMockFileStatus(cityPA, citySF);
    mockListStatusIterator(mockFs, countryUS, filesInUS);

    FileStatus[] filesInInd = getMockFileStatus(cityBOM, cityDEL);
    mockListStatusIterator(mockFs, countryIND, filesInInd);

    //level 3 listing
    FileStatus[] paFiles = getMockFileStatus(paData);
    mockListStatusIterator(mockFs, cityPA, paFiles);

    FileStatus[] sfFiles = getMockFileStatus(sfData);
    mockListStatusIterator(mockFs, citySF, sfFiles);

    FileStatus[] bomFiles = getMockFileStatus(bomData);
    mockListStatusIterator(mockFs, cityBOM, bomFiles);

    FileStatus[] delFiles = getMockFileStatus(delData);
    mockListStatusIterator(mockFs, cityDEL, delFiles);

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(Mockito.mock(IMetaStoreClient.class),
        MetastoreConf.newMetastoreConf());
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Set<Path> result = new HashSet<>();
    checker.checkPartitionDirs(executorService, tableLocation, result, mockFs,
        Arrays.asList("country", "city"));
    // if there are n partition columns, then number of times listStatus should be called
    // must be equal
    // to (numDirsAtLevel1) + (numDirsAtLevel2) + ... + (numDirAtLeveln-1)
    // in this case it should 1 (table level) + 2 (US, IND)
    verify(mockFs, times(3)).listStatusIterator(any(Path.class));
    Assert.assertEquals("msck should have found 4 unknown partitions", 4, result.size());
  }

  private void mockListStatusIterator(LocalFileSystem mockFs, Path location,
      FileStatus[] fileStatuses) throws IOException {
    when(mockFs.listStatusIterator(location)).thenReturn(
      new RemoteIterator<FileStatus>() {
        private int i = 0;
        @Override
        public boolean hasNext() throws IOException {
          return this.i < fileStatuses.length;
        }
        @Override
        public FileStatus next() throws IOException {
          return fileStatuses[this.i++];
        }
      });
  }

  private FileStatus[] getMockFileStatus(Path... paths) throws IOException {
    FileStatus[] result = new FileStatus[paths.length];
    int i = 0;
    for (Path p : paths) {
      result[i++] = createMockFileStatus(p);
    }
    return result;
  }

  private FileStatus createMockFileStatus(Path p) {
    FileStatus mock = Mockito.mock(FileStatus.class);
    when(mock.getPath()).thenReturn(p);
    if (p.toString().contains("datafile")) {
      when(mock.isDirectory()).thenReturn(false);
    } else {
      when(mock.isDirectory()).thenReturn(true);
    }
    return mock;
  }
}
