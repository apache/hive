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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hive.common.util.Ref;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Tests for the SparkUtilities class.
 */
public class TestSparkUtilities {

  @Test
  public void testCreateMoveTaskDoesntCreateCascadeTempDirs() throws Exception {
    FileSinkOperator fsOp = mock(FileSinkOperator.class);
    ParseContext pctx = mock(ParseContext.class);
    Configuration conf = new Configuration();
    conf.set("_hive.hdfs.session.path", "hdfs:/dummypath");
    conf.set("_hive.local.session.path", "hdfs:/dummypath");
    Context ctx = new Context(conf);
    String executionId = ctx.getExecutionId();
    Context ctxSpy = spy(ctx);
    FileSinkDesc fileSinkDesc = mock(FileSinkDesc.class);

    Path mrPath = new Path("hdfs:/tmp/.staging/" + executionId + "/-mr-10001");
    Path mrPath2 = new Path("hdfs:/tmp/.staging/" + executionId + "/-mr-10002");
    Path extPath = new Path("hdfs:/tmp/.staging/" + executionId + "/-ext-10001");
    Path extPath2 = new Path("hdfs:/tmp/.staging/" + executionId + "/-ext-10002");

    final Ref<Path> expectedPathRef = new Ref<>(mrPath);
    final Ref<Path> testPathRef = new Ref<>(extPath);

    doAnswer(invocationOnMock -> {
      return ctxSpy;
    }).when(pctx).getContext();
    doAnswer(invocationOnMock -> {
      return mrPath2;
    }).when(ctxSpy).getMRTmpPath();
    doAnswer(invocationOnMock -> {
      return extPath2;
    }).when(ctxSpy).getExternalTmpPath(any(Path.class));
    doAnswer(invocationOnMock -> {
      return testPathRef.value;
    }).when(fileSinkDesc).getFinalDirName();
    doAnswer(invocationOnMock -> {
      return null;
    }).when(fileSinkDesc).getLinkedFileSinkDesc();
    doAnswer(invocationOnMock -> {
      return fileSinkDesc;
    }).when(fsOp).getConf();

    doAnswer(invocationOnMock -> {
      assertEquals(expectedPathRef.value, invocationOnMock.getArgumentAt(0, Path.class));
      return null;
    }).when(fileSinkDesc).setDirName(any(Path.class));

    testPathRef.value = mrPath;
    expectedPathRef.value = mrPath2;
    GenSparkUtils.createMoveTask(null, true, fsOp, pctx, null, null, null);

    testPathRef.value = extPath;
    expectedPathRef.value = extPath2;
    GenSparkUtils.createMoveTask(null, true, fsOp, pctx, null, null, null);
  }
}
