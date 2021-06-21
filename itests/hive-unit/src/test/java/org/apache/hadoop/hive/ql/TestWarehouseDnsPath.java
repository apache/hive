/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Test;

public class TestWarehouseDnsPath {
  Configuration conf = new Configuration();

  @Test
  public void testDnsPathNullAuthority() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost");
    assertEquals("hdfs://localhost/path/1", transformPath("hdfs:///path/1"));
    conf.set("fs.defaultFS", "s3://bucket");
    assertEquals("s3://bucket/path/1", transformPath("s3:///path/1"));
  }

  @Test
  public void testDnsPathWithAuthority() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost");
    assertEquals("hdfs://127.0.0.1/path/1", transformPath("hdfs://127.0.0.1/path/1"));
    conf.set("fs.defaultFS", "s3a://bucket");
    assertEquals("s3a://bucket/path/1", transformPath("s3a://bucket/path/1"));
  }

  @Test
  public void testDnsPathWithNoSchemeNoAuthority() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost");
    assertEquals("hdfs://localhost/path/1", transformPath("/path/1"));
    conf.set("fs.defaultFS", "s3n://bucket");
    assertEquals("s3n://bucket/path/1", transformPath("/path/1"));
  }

  private String transformPath(String path) throws MetaException {
    return Warehouse.getDnsPath(new Path(path), conf).toString();
  }
}
