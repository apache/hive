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

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HiveThriftServlet extends HttpServlet {
  private final TProcessor processor;
  private final TProtocolFactory inProtocolFactory;
  private final TProtocolFactory outProtocolFactory;

  public HiveThriftServlet(
      TProcessor processor,
      TProtocolFactory inProtocolFactory,
      TProtocolFactory outProtocolFactory
  ) {
    this.processor = processor;
    this.inProtocolFactory = inProtocolFactory;
    this.outProtocolFactory = outProtocolFactory;
  }

  public HiveThriftServlet(TProcessor processor, TProtocolFactory protocolFactory) {
    this(processor, protocolFactory, protocolFactory);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {
    try {
      resp.setContentType("application/x-thrift");
      InputStream in = req.getInputStream();
      OutputStream out = resp.getOutputStream();
      TTransport transport = new TIOStreamTransport(in, out);

      processor.process(inProtocolFactory.getProtocol(transport), outProtocolFactory.getProtocol(transport));
      out.flush();
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doPost(req, resp);
  }
}
