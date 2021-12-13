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

package org.apache.hadoop.hive.llap.cli.service;

import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.api.impl.LlapInputFormat;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.eclipse.jetty.rewrite.handler.Rule;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Copy local jars for the tarball. */
class AsyncTaskCopyLocalJars implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskCopyLocalJars.class.getName());

  private final FileSystem rawFs;
  private final Path libDir;

  AsyncTaskCopyLocalJars(FileSystem rawFs, Path libDir) {
    this.rawFs = rawFs;
    this.libDir = libDir;
  }

  @Override
  public Void call() throws Exception {
    Class<?>[] dependencies = new Class<?>[] {
        LlapDaemonProtocolProtos.class, // llap-common
        LlapTezUtils.class, // llap-tez
        LlapInputFormat.class, // llap-server
        HiveInputFormat.class, // hive-exec
        SslContextFactory.class, // hive-common (https deps)
        Rule.class, // Jetty rewrite class
        RegistryUtils.ServiceRecordMarshal.class, // ZK registry
        // log4j2
        com.lmax.disruptor.RingBuffer.class, // disruptor
        org.apache.logging.log4j.Logger.class, // log4j-api
        org.apache.logging.log4j.core.Appender.class, // log4j-core
        org.apache.logging.slf4j.Log4jLogger.class, // log4j-slf4j
        // log4j-1.2-API needed for NDC
        org.apache.log4j.config.Log4j1ConfigurationFactory.class,
        io.netty.util.NetUtil.class, // netty4
        io.netty.handler.codec.http.HttpObjectAggregator.class, //
        org.apache.arrow.vector.types.pojo.ArrowType.class, //arrow-vector
        org.apache.arrow.memory.RootAllocator.class, //arrow-memory
        org.apache.arrow.memory.NettyAllocationManager.class, //arrow-memory-netty
        io.netty.handler.codec.http.HttpObjectAggregator.class, // netty-all
        org.apache.arrow.flatbuf.Schema.class, //arrow-format
        com.google.flatbuffers.Table.class, //flatbuffers
        com.carrotsearch.hppc.ByteArrayDeque.class, //hppc
        io.jsonwebtoken.security.Keys.class, //jjwt-api
        io.jsonwebtoken.impl.DefaultJws.class, //jjwt-impl
        io.jsonwebtoken.io.JacksonSerializer.class, //jjwt-jackson
    };

    for (Class<?> c : dependencies) {
      Path jarPath = new Path(Utilities.jarFinderGetJar(c));
      rawFs.copyFromLocalFile(jarPath, libDir);
      LOG.debug("Copying " + jarPath + " to " + libDir);
    }
    return null;
  }
}
