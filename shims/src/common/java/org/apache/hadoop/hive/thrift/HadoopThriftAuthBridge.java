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

import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

 /**
  * This class is only overridden by the secure hadoop shim. It allows
  * the Thrift SASL support to bridge to Hadoop's UserGroupInformation
  * infrastructure.
  */
 public class HadoopThriftAuthBridge {
   public Client createClient() {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }

   public Server createServer(String keytabFile, String principalConf)
     throws TTransportException {
     throw new UnsupportedOperationException(
       "The current version of Hadoop does not support Authentication");
   }


   public static abstract class Client {
     public abstract TTransport createClientTransport(
       String principalConfig, String host,
       String methodStr, TTransport underlyingTransport)
       throws IOException;
   }

   public static abstract class Server {
     public abstract TTransportFactory createTransportFactory() throws TTransportException;
     public abstract TProcessor wrapProcessor(TProcessor processor);
   }
 }

