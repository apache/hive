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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * An implementation of ChannelInboundHandler that dispatches incoming messages to an instance
 * method based on the method signature.
 * <p/>
 * A handler's signature must be of the form:
 * <p/>
 * <blockquote><tt>protected void handle(ChannelHandlerContext, MessageType)</tt></blockquote>
 * <p/>
 * Where "MessageType" must match exactly the type of the message to handle. Polymorphism is not
 * supported. Handlers can return a value, which becomes the RPC reply; if a null is returned, then
 * a reply is still sent, with an empty payload.
 */
@InterfaceAudience.Private
public abstract class RpcDispatcher extends SimpleChannelInboundHandler<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(RpcDispatcher.class);

  private final Map<Class<?>, Method> handlers = Maps.newConcurrentMap();
  private final Collection<OutstandingRpc> rpcs = new ConcurrentLinkedQueue<OutstandingRpc>();

  private volatile Rpc.MessageHeader lastHeader;

  /** Override this to add a name to the dispatcher, for debugging purposes. */
  protected String name() {
    return getClass().getSimpleName();
  }

  @Override
  protected final void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (lastHeader == null) {
      if (!(msg instanceof Rpc.MessageHeader)) {
        LOG.warn("[{}] Expected RPC header, got {} instead.", name(),
            msg != null ? msg.getClass().getName() : null);
        throw new IllegalArgumentException();
      }
      lastHeader = (Rpc.MessageHeader) msg;
    } else {
      LOG.debug("[{}] Received RPC message: type={} id={} payload={}", name(),
        lastHeader.type, lastHeader.id, msg != null ? msg.getClass().getName() : null);
      try {
        switch (lastHeader.type) {
        case CALL:
          handleCall(ctx, msg);
          break;
        case REPLY:
          handleReply(ctx, msg, findRpc(lastHeader.id));
          break;
        case ERROR:
          handleError(ctx, msg, findRpc(lastHeader.id));
          break;
        default:
          throw new IllegalArgumentException("Unknown RPC message type: " + lastHeader.type);
        }
      } finally {
        lastHeader = null;
      }
    }
  }

  private OutstandingRpc findRpc(long id) {
    for (Iterator<OutstandingRpc> it = rpcs.iterator(); it.hasNext();) {
      OutstandingRpc rpc = it.next();
      if (rpc.id == id) {
        it.remove();
        return rpc;
      }
    }
    throw new IllegalArgumentException(String.format(
        "Received RPC reply for unknown RPC (%d).", id));
  }

  private void handleCall(ChannelHandlerContext ctx, Object msg) throws Exception {
    Method handler = handlers.get(msg.getClass());
    if (handler == null) {
      handler = getClass().getDeclaredMethod("handle", ChannelHandlerContext.class,
          msg.getClass());
      handler.setAccessible(true);
      handlers.put(msg.getClass(), handler);
    }

    Rpc.MessageType replyType;
    Object replyPayload;
    try {
      replyPayload = handler.invoke(this, ctx, msg);
      if (replyPayload == null) {
        replyPayload = new Rpc.NullMessage();
      }
      replyType = Rpc.MessageType.REPLY;
    } catch (InvocationTargetException ite) {
      LOG.debug(String.format("[%s] Error in RPC handler.", name()), ite.getCause());
      replyPayload = Throwables.getStackTraceAsString(ite.getCause());
      replyType = Rpc.MessageType.ERROR;
    }
    ctx.channel().write(new Rpc.MessageHeader(lastHeader.id, replyType));
    ctx.channel().writeAndFlush(replyPayload);
  }

  private void handleReply(ChannelHandlerContext ctx, Object msg, OutstandingRpc rpc)
      throws Exception {
    rpc.future.setSuccess(msg instanceof Rpc.NullMessage ? null : msg);
  }

  private void handleError(ChannelHandlerContext ctx, Object msg, OutstandingRpc rpc)
      throws Exception {
    if (msg instanceof String) {
      LOG.warn("Received error message:{}.", msg);
      rpc.future.setFailure(new RpcException((String) msg));
    } else {
      String error = String.format("Received error with unexpected payload (%s).",
          msg != null ? msg.getClass().getName() : null);
      LOG.warn(String.format("[%s] %s", name(), error));
      rpc.future.setFailure(new IllegalArgumentException(error));
      ctx.close();
    }
  }

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("[%s] Caught exception in channel pipeline.", name()), cause);
    } else {
      LOG.info("[{}] Closing channel due to exception in pipeline ({}).", name(),
          cause.getMessage());
    }

    if (lastHeader != null) {
      // There's an RPC waiting for a reply. Exception was most probably caught while processing
      // the RPC, so send an error.
      ctx.channel().write(new Rpc.MessageHeader(lastHeader.id, Rpc.MessageType.ERROR));
      ctx.channel().writeAndFlush(Throwables.getStackTraceAsString(cause));
      lastHeader = null;
    }

    ctx.close();
  }

  @Override
  public final void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (rpcs.size() > 0) {
      LOG.warn("[{}] Closing RPC channel with {} outstanding RPCs.", name(), rpcs.size());
      for (OutstandingRpc rpc : rpcs) {
        rpc.future.cancel(true);
      }
    }
    super.channelInactive(ctx);
  }

  void registerRpc(long id, Promise promise, String type) {
    LOG.debug("[{}] Registered outstanding rpc {} ({}).", name(), id, type);
    rpcs.add(new OutstandingRpc(id, promise));
  }

  void discardRpc(long id) {
    LOG.debug("[{}] Discarding failed RPC {}.", name(), id);
    findRpc(id);
  }

  private static class OutstandingRpc {
    final long id;
    final Promise future;

    OutstandingRpc(long id, Promise future) {
      this.id = id;
      this.future = future;
    }
  }

}
