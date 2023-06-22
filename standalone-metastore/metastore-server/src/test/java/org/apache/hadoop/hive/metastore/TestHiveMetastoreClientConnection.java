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

package org.apache.hadoop.hive.metastore;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public class TestHiveMetastoreClientConnection {
  private Configuration conf;

  /** Test class mock the connection exception for http and binary mode. */
  static class HiveMetastoreClientWithException extends HiveMetaStoreClient {
    private static int connectCounts = 0;

    public HiveMetastoreClientWithException(Configuration conf) throws MetaException {
      super(conf);
    }

    @Override
    protected TTransport createAuthBinaryTransport(URI store, TTransport underlyingTransport)
        throws MetaException, TTransportException {
      connectCounts++;
      throw new TTransportException();
    }

    @Override
    protected HttpClientBuilder createHttpClientBuilder() throws MetaException {
      connectCounts++;
      throw new MetaException();
    }
  }

  @Before
  public void setup() {
    HiveMetastoreClientWithException.connectCounts = 0;
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:1,thrift://localhost:2");
  }

  @Test
  public void testConnectionWithMetaException() {
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");
    try {
      new HiveMetastoreClientWithException(conf);
      Assert.fail("Instantiate client should fail by MetaException for http mode.");
    } catch (MetaException e) {
      // expected
      Assert.assertTrue(
          e.getMessage().contains("Could not connect to meta store by MetaException:"));
    }
    Assert.assertEquals(HiveMetastoreClientWithException.connectCounts, 1);
  }

  @Test
  public void testConnectionWithTTransportException() {
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "binary");
    try {
      new HiveMetastoreClientWithException(conf);
      Assert.fail("Instantiate client should fail by TTransportException for binary mode.");
    } catch (MetaException e) {
      // expected
      Assert.assertTrue(
          e.getMessage().contains("Could not connect to meta store using any of the URIs provided."));
    }
    Assert.assertEquals(HiveMetastoreClientWithException.connectCounts, 6);
  }
}
