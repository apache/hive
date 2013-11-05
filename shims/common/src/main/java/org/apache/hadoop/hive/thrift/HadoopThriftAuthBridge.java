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

 package org.apache.hadoop.hive.thrift;

 import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

 /**
  * This class is only overridden by the secure hadoop shim. It allows
  * the Thrift SASL support to bridge to Hadoop's UserGroupInformation
  * & DelegationToken infrastructure.
  */
 public class HadoopThriftAuthBridge {
   public Client createClient() {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }

   public Client createClientWithConf(String authType) {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }

   public Server createServer(String keytabFile, String principalConf)
     throws TTransportException {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }


  /**
   * Read and return Hadoop SASL configuration which can be configured using
   * "hadoop.rpc.protection"
   *
   * @param conf
   * @return Hadoop SASL configuration
   */
   public Map<String, String> getHadoopSaslProperties(Configuration conf) {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }

   public static abstract class Client {
   /**
    *
    * @param principalConfig In the case of Kerberos authentication this will
    * be the kerberos principal name, for DIGEST-MD5 (delegation token) based
    * authentication this will be null
    * @param host The metastore server host name
    * @param methodStr "KERBEROS" or "DIGEST"
    * @param tokenStrForm This is url encoded string form of
    * org.apache.hadoop.security.token.
    * @param underlyingTransport the underlying transport
    * @return the transport
    * @throws IOException
    */
     public abstract TTransport createClientTransport(
             String principalConfig, String host,
             String methodStr, String tokenStrForm, TTransport underlyingTransport,
             Map<String, String> saslProps)
             throws IOException;
   }

   public static abstract class Server {
     public abstract TTransportFactory createTransportFactory(Map<String, String> saslProps) throws TTransportException;
     public abstract TProcessor wrapProcessor(TProcessor processor);
     public abstract TProcessor wrapNonAssumingProcessor(TProcessor processor);
     public abstract InetAddress getRemoteAddress();
     public abstract void startDelegationTokenSecretManager(Configuration conf,
       Object hmsHandler) throws IOException;
     public abstract String getRemoteUser();
     public abstract String getDelegationToken(String owner, String renewer)
     throws IOException, InterruptedException;
     public abstract long renewDelegationToken(String tokenStrForm) throws IOException;
     public abstract void cancelDelegationToken(String tokenStrForm) throws IOException;
   }
 }

