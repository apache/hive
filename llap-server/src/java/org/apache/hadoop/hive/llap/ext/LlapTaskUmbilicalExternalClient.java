package org.apache.hadoop.hive.llap.ext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.helpers.LlapTaskUmbilicalServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskUmbilicalExternalClient extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskUmbilicalExternalClient.class);

  private final LlapProtocolClientProxy communicator;
  private volatile LlapTaskUmbilicalServer llapTaskUmbilicalServer;
  private final Configuration conf;
  private final LlapTaskUmbilicalProtocol umbilical;

  protected final String tokenIdentifier;
  protected final Token<JobTokenIdentifier> sessionToken;


  private final ConcurrentMap<String, List<TezEvent>> pendingEvents = new ConcurrentHashMap<>();


  // TODO KKK Work out the details of the tokenIdentifier, and the session token.
  // It may just be possible to create one here - since Shuffle is not involved, and this is only used
  // for communication from LLAP-Daemons to the server. It will need to be sent in as part
  // of the job submission request.
  public LlapTaskUmbilicalExternalClient(Configuration conf, String tokenIdentifier, Token<JobTokenIdentifier> sessionToken) {
    super(LlapTaskUmbilicalExternalClient.class.getName());
    this.conf = conf;
    this.umbilical = new LlapTaskUmbilicalExternalImpl();
    this.tokenIdentifier = tokenIdentifier;
    this.sessionToken = sessionToken;
    // TODO. No support for the LLAP token yet. Add support for configurable threads, however 1 should always be enough.
    this.communicator = new LlapProtocolClientProxy(1, conf, null);
  }

  @Override
  public void serviceStart() throws IOException {
    int numHandlers = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.LLAP_TMP_EXT_CLIENT_NUM_SERVER_HANDLERS);
    llapTaskUmbilicalServer = new LlapTaskUmbilicalServer(conf, umbilical, numHandlers, tokenIdentifier, sessionToken);
  }

  @Override
  public void serviceStop() {
    llapTaskUmbilicalServer.shutdownServer();
    if (this.communicator != null) {
      this.communicator.stop();
    }
  }


  /**
   * Submit the work for actual execution. This should always have the usingTezAm flag disabled
   * @param submitWorkRequestProto
   */
  public void submitWork(final SubmitWorkRequestProto submitWorkRequestProto, String llapHost, int llapPort) {
    Preconditions.checkArgument(submitWorkRequestProto.getUsingTezAm() == false);

    // Store the actual event first. To be returned on the first heartbeat.
    Event mrInputEvent = null;
    // Construct a TezEvent out of this, to send it out on the next heaertbeat

//    submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString()


    // Send out the actual SubmitWorkRequest
    communicator.sendSubmitWork(submitWorkRequestProto, llapHost, llapPort,
        new LlapProtocolClientProxy.ExecuteRequestCallback<LlapDaemonProtocolProtos.SubmitWorkResponseProto>() {
          @Override
          public void setResponse(LlapDaemonProtocolProtos.SubmitWorkResponseProto response) {
            if (response.hasSubmissionState()) {
              if (response.getSubmissionState().equals(LlapDaemonProtocolProtos.SubmissionStateProto.REJECTED)) {
                LOG.info("Fragment: " + submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString() + " rejected. Server Busy.");
                return;
              }
            }
            LOG.info("DBG: Submitted " + submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString());
          }

          @Override
          public void indicateError(Throwable t) {
            LOG.error("Failed to submit: " + submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString(), t);
          }
        });




//    // TODO Also send out information saying that the fragment is finishable - if that is not already included in the main fragment.
//    // This entire call is only required if we're doing more than scans. MRInput has no dependencies and is always finishable
//    QueryIdentifierProto queryIdentifier = QueryIdentifierProto
//        .newBuilder()
//        .setAppIdentifier(submitWorkRequestProto.getApplicationIdString()).setDagIdentifier(submitWorkRequestProto.getFragmentSpec().getDagId())
//        .build();
//    LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto sourceStateUpdatedRequest =
//        LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto.newBuilder().setQueryIdentifier(queryIdentifier).setState(
//            LlapDaemonProtocolProtos.SourceStateProto.S_SUCCEEDED).
//            setSrcName(TODO)
//    communicator.sendSourceStateUpdate(LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto.newBuilder().setQueryIdentifier(submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString()).set);


  }







  // TODO Ideally, the server should be shared across all client sessions running on the same node.
  private class LlapTaskUmbilicalExternalImpl implements  LlapTaskUmbilicalProtocol {

    @Override
    public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
      // Expecting only a single instance of a task to be running.
      return true;
    }

    @Override
    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) throws IOException,
        TezException {
      // Keep-alive information. The client should be informed and will have to take care of re-submitting the work.
      // Some parts of fault tolerance go here.

      // This also provides completion information, and a possible notification when task actually starts running (first heartbeat)

      // Incoming events can be ignored until the point when shuffle needs to be handled, instead of just scans.


      TezHeartbeatResponse response = new TezHeartbeatResponse();
      // Assuming TaskAttemptId and FragmentIdentifierString are the same. Verify this.
      TezTaskAttemptID taskAttemptId = request.getCurrentTaskAttemptID();
      LOG.info("ZZZ: DBG: Received heartbeat from taskAttemptId: " + taskAttemptId.toString());

      List<TezEvent> tezEvents = pendingEvents.remove(taskAttemptId.toString());

      response.setLastRequestId(request.getRequestId());
      // Irrelevant from eventIds. This can be tracked in the AM itself, instead of polluting the task.
      // Also since we have all the MRInput events here - they'll all be sent in together.
      response.setNextFromEventId(0); // Irrelevant. See comment above.
      response.setNextPreRoutedEventId(0); //Irrelevant. See comment above.
      response.setEvents(tezEvents);

      // TODO KKK: Should ideally handle things like Task success notifications.
      // Can this somehow be hooked into the LlapTaskCommunicator to make testing easy

      return response;
    }

    @Override
    public void nodeHeartbeat(Text hostname, int port) throws IOException {
      // TODO Eventually implement - to handle keep-alive messages from pending work.
    }

    @Override
    public void taskKilled(TezTaskAttemptID taskAttemptId) throws IOException {
      // TODO Eventually implement - to handle preemptions within LLAP daemons.
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, protocol,
          clientVersion, clientMethodsHash);
    }
  }

}
