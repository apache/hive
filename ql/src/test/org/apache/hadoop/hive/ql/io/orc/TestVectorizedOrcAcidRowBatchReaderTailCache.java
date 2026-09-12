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
package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader.ReaderData;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcTail;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;

/**
 * The delete delta tail served from the LLAP metadata cache has to be the one the reader uses:
 * re-reading it from the filesystem makes hive.llap.io.cache.deletedeltas=metadata do nothing and
 * costs a footer read per split at the 'all' level.
 */
public class TestVectorizedOrcAcidRowBatchReaderTailCache {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Path path;
  private JobConf conf;

  @Before
  public void writeOrcFile() throws Exception {
    File file = new File(temp.getRoot(), "delete_delta_file.orc");
    path = new Path(file.toURI());
    conf = new JobConf();
    TypeDescription schema = TypeDescription.fromString("struct<id:int>");
    try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema))) {
      org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch batch = schema.createRowBatch();
      ((org.apache.hadoop.hive.ql.exec.vector.LongColumnVector) batch.cols[0]).vector[0] = 1;
      batch.size = 1;
      writer.addRowBatch(batch);
    }
    HiveConf.setVar(conf, ConfVars.HIVE_EXECUTION_MODE, "llap");
    HiveConf.setVar(conf, ConfVars.LLAP_IO_CACHE_DELETEDELTAS, "metadata");
  }

  @Test
  public void testCachedTailIsTheOneReturned() throws Exception {
    OrcTail cachedTail;
    try (org.apache.orc.Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))) {
      cachedTail = new OrcTail(reader.getFileTail(), reader.getSerializedFileFooter());
    }
    LlapIo<?> llapIo = mock(LlapIo.class);
    when(llapIo.getOrcTailFromCache(any(), any(), any(), any())).thenReturn(cachedTail);

    try (MockedStatic<LlapProxy> proxy = mockStatic(LlapProxy.class)) {
      proxy.when(LlapProxy::isDaemon).thenReturn(true);
      proxy.when(LlapProxy::getIo).thenReturn(llapIo);

      ReaderData readerData = VectorizedOrcAcidRowBatchReader.getOrcReaderData(path, conf, null, null);

      assertSame("the cached tail must not be replaced by a filesystem read", cachedTail, readerData.orcTail);
      assertNotNull(readerData.reader);
    }
  }
}
