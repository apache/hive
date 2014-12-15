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
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import javax.security.auth.callback.CallbackHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public UserGroupInformation getCurrentUGIWithConf(String authType)
      throws IOException {
    throw new UnsupportedOperationException(
        "The current version of Hadoop does not support Authentication");
  }


  public String getServerPrincipal(String principalConfig, String host)
      throws IOException {
    throw new UnsupportedOperationException(
        "The current version of Hadoop does not support Authentication");
  }

  public Server createServer(String keytabFile, String principalConf) throws TTransportException {
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
    public enum ServerMode {
      HIVESERVER2, METASTORE
    };

    public abstract TTransportFactory createTransportFactory(Map<String, String> saslProps,
        int saslMessageLimit) throws TTransportException;
    public abstract TProcessor wrapProcessor(TProcessor processor);
    public abstract TProcessor wrapNonAssumingProcessor(TProcessor processor);
    public abstract InetAddress getRemoteAddress();
    public abstract void startDelegationTokenSecretManager(Configuration conf,
        Object hmsHandler, ServerMode smode) throws IOException;
    public abstract String getDelegationToken(String owner, String renewer)
        throws IOException, InterruptedException;
    public abstract String getDelegationTokenWithService(String owner, String renewer, String service)
        throws IOException, InterruptedException;
    public abstract String getRemoteUser();
    public abstract long renewDelegationToken(String tokenStrForm) throws IOException;
    public abstract void cancelDelegationToken(String tokenStrForm) throws IOException;
    public abstract String getUserFromToken(String tokenStr) throws IOException;
  }

  public static class HiveSaslServerTransportFactory extends TSaslServerTransport.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSaslServerTransport.class);
    private final int saslMessageLimit;

    public HiveSaslServerTransportFactory(int saslMessageLimit) {
      this.saslMessageLimit = saslMessageLimit;
    }

    private static class TSaslServerDefinition {
      public String mechanism;
      public String protocol;
      public String serverName;
      public Map<String, String> props;
      public CallbackHandler cbh;

      public TSaslServerDefinition(String mechanism, String protocol, String serverName,
          Map<String, String> props, CallbackHandler cbh) {
        this.mechanism = mechanism;
        this.protocol = protocol;
        this.serverName = serverName;
        this.props = props;
        this.cbh = cbh;
      }
    }

    private static Map<TTransport, WeakReference<TSaslServerTransport>> transportMap = Collections
        .synchronizedMap(new WeakHashMap<TTransport, WeakReference<TSaslServerTransport>>());
    private Map<String, TSaslServerDefinition> serverDefinitionMap =
        new HashMap<String, TSaslServerDefinition>();

    public void addServerDefinition(String mechanism, String protocol, String serverName,
        Map<String, String> props, CallbackHandler cbh) {
      serverDefinitionMap.put(mechanism, new TSaslServerDefinition(mechanism, protocol, serverName,
          props, cbh));
    }

    @Override
    public TTransport getTransport(TTransport base) {
      WeakReference<TSaslServerTransport> ret = transportMap.get(base);
      TSaslServerTransport transport = ret == null ? null : ret.get();
      if (transport == null) {
        LOGGER.debug("transport map does not contain key {}", base);
        transport = newSaslTransport(base);
        try {
          transport.open();
        } catch (TTransportException e) {
          LOGGER.debug("failed to open server transport", e);
          throw new RuntimeException(e);
        }
        transportMap.put(base, new WeakReference<TSaslServerTransport>(transport));
      } else {
        LOGGER.debug("transport map does contain key {}", base);
      }
      return transport;
    }

    private TSaslServerTransport newSaslTransport(final TTransport base) {
      // Anonymous subclass of TSaslServerTransport. TSaslServerTransport#recieveSaslMessage
      // is replaced with one that has additional check for the message size.
      TSaslServerTransport transport = new TSaslServerTransport(base) {
        private final byte[] messageHeader = new byte[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];

        @Override
        protected SaslResponse receiveSaslMessage() throws TTransportException {
          underlyingTransport.readAll(messageHeader, 0, messageHeader.length);
          byte statusByte = messageHeader[0];
          int length = EncodingUtils.decodeBigEndian(messageHeader, STATUS_BYTES);
          if (length > saslMessageLimit) {
            base.close();
            throw new TTransportException("Sasl message is too big (" + length + " bytes). "
                + "The peer connection is possibly using a protocol other than thrift.");
          }
          byte[] payload = new byte[length];
          underlyingTransport.readAll(payload, 0, payload.length);
          NegotiationStatus status = NegotiationStatus.byValue(statusByte);
          if (status == null) {
            sendAndThrowMessage(NegotiationStatus.ERROR, "Invalid status " + statusByte);
          } else if (status == NegotiationStatus.BAD || status == NegotiationStatus.ERROR) {
            try {
              String remoteMessage = new String(payload, "UTF-8");
              throw new TTransportException("Peer indicated failure: " + remoteMessage);
            } catch (UnsupportedEncodingException e) {
              throw new TTransportException(e);
            }
          }
          if (LOGGER.isDebugEnabled())
            LOGGER.debug(getRole() + ": Received message with status {} and payload length {}",
                status, payload.length);
          return new SaslResponse(status, payload);
        }
      };
      for (Map.Entry<String, TSaslServerDefinition> entry : serverDefinitionMap.entrySet()) {
        TSaslServerDefinition definition = entry.getValue();
        transport.addServerDefinition(entry.getKey(), definition.protocol, definition.serverName,
            definition.props, definition.cbh);
      }
      return transport;
    }
  }
}

