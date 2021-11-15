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

package org.apache.hadoop.hive.ql.io.orc.encoded;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.MockFileSystem;
import org.apache.orc.CompressionKind;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.Footer;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.OrcTail;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to verify lazy nature of EncodedOrcFile.
 */
public class TestEncodedOrcFile {

  @Test
  public void testFileSystemIsNotInitializedWithKnownTail() throws IOException {
    JobConf conf = new JobConf();
    Path path = new Path("fmock:///testtable/bucket_0");
    conf.set("hive.orc.splits.include.file.footer", "true");
    conf.set("fs.defaultFS", "fmock:///");
    conf.set("fs.mock.impl", FailingMockFileSystem.class.getName());

    OrcProto.FileTail tail = OrcProto.FileTail.newBuilder()
        .setFooter(Footer.newBuilder()
            .addTypes(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.BINARY).build())
            .build())
        .build();
    OrcFile.ReaderOptions readerOptions = EncodedOrcFile.readerOptions(conf)
        .filesystem(() -> {
          throw new RuntimeException("Filesystem should not have been initialized");
        })
        .orcTail(new OrcTail(tail, new BufferChunk(0, 0), -1));

    // an orc reader is created, this should not cause filesystem initialization
    // because orc tail is already provided and we are not making any real reads.
    Reader reader = EncodedOrcFile.createReader(path, readerOptions);

    // Following initiates the creation of data reader in ORC reader. This should
    // not cause file system initialization either as we are still not making any
    // real read.
    reader.rows();
  }

  private static class FailingMockFileSystem extends MockFileSystem {
    @Override
    public void initialize(URI uri, Configuration conf) {
      throw new RuntimeException("Filesystem should not have been initialized");
    }
  }
}
