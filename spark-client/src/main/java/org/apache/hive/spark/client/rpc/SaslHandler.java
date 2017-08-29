/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.rpc;

import java.io.IOException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract SASL handler. Abstracts the auth protocol handling and encryption, if it's enabled.
 * Needs subclasses to provide access to the actual underlying SASL implementation (client or
 * server).
 */
abstract class SaslHandler extends SimpleChannelInboundHandler<Rpc.SaslMessage>
    implements KryoMessageCodec.EncryptionHandler {

  // LOG is not static to make debugging easier (being able to identify which sub-class
  // generated the log message).
  protected final Logger LOG;
  private final boolean requiresEncryption;
  private KryoMessageCodec kryo;
  private boolean hasAuthResponse = false;

  protected SaslHandler(RpcConfiguration config) {
    this.requiresEncryption = Rpc.SASL_AUTH_CONF.equals(config.getSaslOptions().get(Sasl.QOP));
    this.LOG = LoggerFactory.getLogger(getClass());
  }

  // Use a separate method to make it easier to create a SaslHandler without having to
  // plumb the KryoMessageCodec instance through the constructors.
  void setKryoMessageCodec(KryoMessageCodec kryo) {
    this.kryo = kryo;
  }

  @Override
  protected final void channelRead0(ChannelHandlerContext ctx, Rpc.SaslMessage msg)
      throws Exception {
    LOG.debug("Handling SASL challenge message...");
    Rpc.SaslMessage response = update(msg);
    if (response != null) {
      LOG.debug("Sending SASL challenge response...");
      hasAuthResponse = true;
      ctx.channel().writeAndFlush(response).sync();
    }

    if (!isComplete()) {
      return;
    }

    // If negotiation is complete, remove this handler from the pipeline, and register it with
    // the Kryo instance to handle encryption if needed.
    ctx.channel().pipeline().remove(this);
    String qop = getNegotiatedProperty(Sasl.QOP);
    LOG.debug("SASL negotiation finished with QOP {}.", qop);
    if (Rpc.SASL_AUTH_CONF.equals(qop)) {
      LOG.info("SASL confidentiality enabled.");
      kryo.setEncryptionHandler(this);
    } else {
      if (requiresEncryption) {
        throw new SaslException("Encryption required, but SASL negotiation did not set it up.");
      }
      dispose();
    }

    onComplete();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    dispose();
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!isComplete()) {
      LOG.info("Exception in SASL negotiation.", cause);
      onError(cause);
      ctx.close();
    }
    ctx.fireExceptionCaught(cause);
  }

  protected abstract boolean isComplete();

  protected abstract String getNegotiatedProperty(String name);

  protected abstract Rpc.SaslMessage update(Rpc.SaslMessage challenge) throws IOException;

  protected abstract void onComplete() throws Exception;

  protected abstract void onError(Throwable t);

}
