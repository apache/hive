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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import org.junit.Assert;

@Category(MetastoreCheckinTest.class)
public class TestHiveMetastoreHttpHeaders {
  private static Configuration conf;
  private static HiveMetaStoreClient msc;
  private static int port;
  private static final String testHeaderKey1 = "X-XXXX";
  private static final String testHeaderVal1 = "yyyy";
  private static final String testHeaderKey2 = "X-ZZZZ";
  private static final String testHeaderVal2 = "aaaa";

  static class TestHiveMetaStoreClient extends HiveMetaStoreClient {
    public TestHiveMetaStoreClient(Configuration conf) throws MetaException {
      super(conf);
    }

    @Override
    protected HttpClientBuilder createHttpClientBuilder() throws MetaException {
      HttpClientBuilder builder = super.createHttpClientBuilder();
      builder.addInterceptorLast(new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
          Header header1 = httpRequest.getFirstHeader(testHeaderKey1);
          Assert.assertEquals(testHeaderVal1, header1.getValue());
          Header header2 = httpRequest.getFirstHeader(testHeaderKey2);
          Assert.assertEquals(testHeaderVal2, header2.getValue());
        }
      });
      return builder;
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_TRANSPORT_MODE, "http");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");
    port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
  }

  @Test
  public void testHttpHeaders() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_ADDITIONAL_HEADERS,
        String.format("%s=%s,%s=%s", testHeaderKey1, testHeaderVal1, testHeaderKey2, testHeaderVal2));
    msc = new TestHiveMetaStoreClient(conf);
    Database db = new DatabaseBuilder().setName("testHttpHeader").create(msc, conf);
    msc.dropDatabase(db.getName());
  }

  @Test
  public void testIllegalHttpHeaders() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_ADDITIONAL_HEADERS,
        String.format("%s%s", testHeaderKey1, testHeaderVal1));
    msc = new TestHiveMetaStoreClient(conf);
    boolean exceptionThrown = false;
    try {
      Database db = new DatabaseBuilder().setName("testHttpHeader").create(msc, conf);
      msc.dropDatabase(db.getName());
    } catch (Exception e) {
      exceptionThrown = true;
    }
    Assert.assertTrue("Illegal header should invoke thrown exception", exceptionThrown);
  }
}
