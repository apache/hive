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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TestProxyFileSystem
 */
public class TestProxyFileSystem {

  @Test
  public void testListStatusIterator() throws IOException {
    FileStatus fileStatus = mock(FileStatus.class);
    Path targetPath = new Path(Paths.get("").toAbsolutePath().toString());

    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.getUri()).thenReturn(URI.create("file:///"));

    ProxyFileSystem proxyFileSystem = new ProxyFileSystem(fileSystem, URI.create("pfile:///"));
    when(fileStatus.getPath()).thenReturn(targetPath);

    when(fileSystem.listStatusIterator(any(Path.class))).thenReturn(
        new RemoteIterator<FileStatus>() {
          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public FileStatus next() {
            return fileStatus;
          }
        });

    RemoteIterator<FileStatus> iterator = proxyFileSystem.listStatusIterator(targetPath);
    assertThat(iterator.hasNext(), equalTo(true));

    FileStatus resultStatus = iterator.next();

    assertNotNull(resultStatus);
    assertThat(resultStatus.getPath().toString(), equalTo("pfile:" + targetPath));
  }
}
