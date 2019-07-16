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
package org.apache.hadoop.hive.ql.exec.repl.dump.io;

  import org.apache.hadoop.fs.FSDataOutputStream;
  import org.apache.hadoop.fs.FileSystem;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.hive.conf.HiveConf;
  import org.apache.hadoop.hive.metastore.api.Function;
  import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
  import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
  import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
  import org.junit.Test;
  import org.junit.runner.RunWith;
  import org.mockito.Mock;
  import org.powermock.core.classloader.annotations.PowerMockIgnore;
  import org.powermock.core.classloader.annotations.PrepareForTest;
  import org.powermock.modules.junit4.PowerMockRunner;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

  import static org.mockito.Mockito.mock;
  import static org.powermock.api.mockito.PowerMockito.when;

  @RunWith(PowerMockRunner.class)
  @PrepareForTest({ FunctionSerializer.class})
  @PowerMockIgnore({ "javax.management.*" })
  public class TestFunctionSerializer {

    protected static final Logger LOG = LoggerFactory.getLogger(TestFunctionSerializer.class);

    @Mock
    private Function function;

    @Mock
    private FileSystem fs;

    @Mock
    private Path writePath;
    
    @Test
    public void testWithFunctionHavingNullResourceUri() throws Exception {
      when(function.getResourceUris()).thenReturn(null);
      when(fs.create(writePath)).thenReturn(mock(FSDataOutputStream.class));
      JsonWriter jsonWriter = new JsonWriter(fs, writePath);


      FunctionSerializer funcSerialier = new FunctionSerializer(function, mock(HiveConf.class));
      funcSerialier.writeTo(jsonWriter, mock(ReplicationSpec.class));
    }
  }
