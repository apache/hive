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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.set_ugi_args;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.set_ugi_result;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.TUGIContainingTransport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

/** TUGIBasedProcessor is used in unsecure mode for thrift metastore client server communication.
 *  This processor checks whether the first rpc call after connection is set up is set_ugi()
 *  through which client sends ugi to server. Processor then perform all subsequent rpcs on the
 *  connection using ugi.doAs() so all actions are performed in client user context.
 *  Note that old clients will never call set_ugi() and thus ugi will never be received on server
 *  side, in which case server exhibits previous behavior and continues as usual.
 */
@SuppressWarnings("rawtypes")
public class TUGIBasedProcessor<I extends Iface> extends TSetIpAddressProcessor<Iface> {

  private final I iface;
  private final Map<String,  org.apache.thrift.ProcessFunction<Iface, ? extends  TBase>>
    functions;
  private final HadoopShims shim;

  public TUGIBasedProcessor(I iface) throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
    InvocationTargetException {
    super(iface);
    this.iface = iface;
    this.functions = getProcessMapView();
    shim = ShimLoader.getHadoopShims();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setIpAddress(in);

    final TMessage msg = in.readMessageBegin();
    final ProcessFunction<Iface, ? extends  TBase> fn = functions.get(msg.name);
    if (fn == null) {
      TProtocolUtil.skip(in, TType.STRUCT);
      in.readMessageEnd();
      TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD,
          "Invalid method name: '"+msg.name+"'");
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
      return true;
    }
    TUGIContainingTransport ugiTrans = (TUGIContainingTransport)in.getTransport();
    // Store ugi in transport if the rpc is set_ugi
    if (msg.name.equalsIgnoreCase("set_ugi")){
      try {
        handleSetUGI(ugiTrans, (set_ugi<Iface>)fn, msg, in, out);
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        throw new TException(e.getCause());
      }
      return true;
    }
    UserGroupInformation clientUgi = ugiTrans.getClientUGI();
    if (null == clientUgi){
      // At this point, transport must contain client ugi, if it doesn't then its an old client.
      fn.process(msg.seqid, in, out, iface);
      return true;
    } else { // Found ugi, perform doAs().
      PrivilegedExceptionAction<Void> pvea = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() {
          try {
            fn.process(msg.seqid,in, out, iface);
            return null;
          } catch (TException te) {
            throw new RuntimeException(te);
          }
        }
      };
      try {
        shim.doAs(clientUgi, pvea);
        return true;
      } catch (RuntimeException rte) {
        if (rte.getCause() instanceof TException) {
          throw (TException)rte.getCause();
        }
        throw rte;
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie); // unexpected!
      } catch (IOException ioe) {
        throw new RuntimeException(ioe); // unexpected!
      } finally {
          shim.closeAllForUGI(clientUgi);
      }
    }
  }

  private void handleSetUGI(TUGIContainingTransport ugiTrans,
      set_ugi<Iface> fn, TMessage msg, TProtocol iprot, TProtocol oprot)
      throws TException, SecurityException, NoSuchMethodException, IllegalArgumentException,
      IllegalAccessException, InvocationTargetException{

    UserGroupInformation clientUgi = ugiTrans.getClientUGI();
    if( null != clientUgi){
      throw new TException(new IllegalStateException("UGI is already set. Resetting is not " +
      "allowed. Current ugi is: " + clientUgi.getUserName()));
    }

    set_ugi_args args = fn.getEmptyArgsInstance();
    try {
      args.read(iprot);
    } catch (TProtocolException e) {
      iprot.readMessageEnd();
      TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR,
          e.getMessage());
      oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
      return;
    }
    iprot.readMessageEnd();
    set_ugi_result result = fn.getResult(iface, args);
    List<String> principals = result.getSuccess();
    // Store the ugi in transport and then continue as usual.
    ugiTrans.setClientUGI(shim.createRemoteUser(principals.remove(principals.size()-1),
        principals));
    oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.REPLY, msg.seqid));
    result.write(oprot);
    oprot.writeMessageEnd();
    oprot.getTransport().flush();
  }

  @Override
  protected void setIpAddress(final TProtocol in) {
    TUGIContainingTransport ugiTrans = (TUGIContainingTransport)in.getTransport();
    Socket socket = ugiTrans.getSocket();
    if (socket != null) {
      setIpAddress(socket);
    }
  }
}
