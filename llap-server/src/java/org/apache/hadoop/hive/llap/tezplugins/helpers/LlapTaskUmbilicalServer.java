package org.apache.hadoop.hive.llap.tezplugins.helpers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskUmbilicalServer {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskUmbilicalServer.class);

  protected volatile Server server;
  private final InetSocketAddress address;
  private final AtomicBoolean started = new AtomicBoolean(true);

  public LlapTaskUmbilicalServer(Configuration conf, LlapTaskUmbilicalProtocol umbilical, int numHandlers, String tokenIdentifier, Token<JobTokenIdentifier> token) throws
      IOException {
    JobTokenSecretManager jobTokenSecretManager =
        new JobTokenSecretManager();
    jobTokenSecretManager.addTokenForJob(tokenIdentifier, token);

    server = new RPC.Builder(conf)
        .setProtocol(LlapTaskUmbilicalProtocol.class)
        .setBindAddress("0.0.0.0")
        .setPort(0)
        .setInstance(umbilical)
        .setNumHandlers(numHandlers)
        .setSecretManager(jobTokenSecretManager).build();

    server.start();
    this.address = NetUtils.getConnectAddress(server);
    LOG.info(
        "Started TaskUmbilicalServer: " + umbilical.getClass().getName() + " at address: " + address +
            " with numHandlers=" + numHandlers);
  }

  public InetSocketAddress getAddress() {
    return this.address;
  }

  public void shutdownServer() {
    if (started.get()) { // Primarily to avoid multiple shutdowns.
      started.set(false);
      server.stop();
    }
  }
}
