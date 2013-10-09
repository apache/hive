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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;

public class TUGIContainingProcessor implements TProcessor{

  private final TProcessor wrapped;
  private final HadoopShims shim;
  private final boolean isFsCacheDisabled;

  public TUGIContainingProcessor(TProcessor wrapped, Configuration conf) {
    this.wrapped = wrapped;
    this.isFsCacheDisabled = conf.getBoolean(String.format("fs.%s.impl.disable.cache",
      FileSystem.getDefaultUri(conf).getScheme()), false);
    this.shim = ShimLoader.getHadoopShims();
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    UserGroupInformation clientUgi = null;

    try {
      clientUgi = shim.createRemoteUser(((TSaslServerTransport)in.getTransport()).
          getSaslServer().getAuthorizationID(), new ArrayList<String>());
      return shim.doAs(clientUgi, new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() {
          try {
            return wrapped.process(in, out);
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
