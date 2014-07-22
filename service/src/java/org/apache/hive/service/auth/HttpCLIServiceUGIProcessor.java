/**
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

package org.apache.hive.service.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;

/**
 *
 * Wraps the underlying thrift processor's process call,
 * to assume the client user's UGI/Subject for the doAs calls.
 * Gets the client's username from a threadlocal in SessionManager which is
 * set in the ThriftHttpServlet, and constructs a client UGI object from that.
 *
 */

public class HttpCLIServiceUGIProcessor implements TProcessor {

  private final TProcessor underlyingProcessor;
  private final HadoopShims shim;

  public HttpCLIServiceUGIProcessor(TProcessor underlyingProcessor) {
    this.underlyingProcessor = underlyingProcessor;
    this.shim = ShimLoader.getHadoopShims();
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    /**
     * Build the client UGI from threadlocal username [SessionManager.getUserName()].
     * The threadlocal username is set in the ThriftHttpServlet.
     */
    UserGroupInformation clientUgi = null;
    try {
      clientUgi = shim.createRemoteUser(SessionManager.getUserName(), new ArrayList<String>());
      return shim.doAs(clientUgi, new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() {
          try {
            return underlyingProcessor.process(in, out);
          } catch (TException te) {
            throw new RuntimeException(te);
          }
        }
      });
    }
    catch (RuntimeException rte) {
      if (rte.getCause() instanceof TException) {
        throw (TException)rte.getCause();
      }
      throw rte;
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie); // unexpected!
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); // unexpected!
    }
  }
}
