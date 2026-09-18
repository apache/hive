/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap.tezplugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Concurrency test for the LLAP plugin broker.
 *
 * <p>Fans out N pairs of {@link LlapTaskCommunicator} + {@link LlapTaskSchedulerService}
 * constructions in parallel, each pair sharing a unique {@link ApplicationAttemptId}. All 2N
 * constructor threads release from a common barrier so their {@link LlapPluginBroker}
 * pair-or-park sections interleave as aggressively as the JVM will let them. After every
 * construction has completed, the test asserts that every communicator's paired {@code scheduler}
 * field refers to the scheduler for the <i>same</i> {@code ApplicationAttemptId} — i.e. no
 * cross-DAG mis-pairing.
 *
 * <p>An earlier version of both plugins used a single class-static {@code instance} slot for the
 * cross-plugin handshake and routinely mis-paired under this workload; the surfacing symptom in
 * production was {@code NullPointerException: … scheduler is null} from
 * {@code LlapTaskCommunicator$3.setResponse}.
 */
public class TestLlapPluginBroker {

  private static final int PAIR_COUNT = 32;
  private static final int ITERATIONS = 25;
  private static final long AWAIT_TIMEOUT_SECONDS = 60;

  private ExecutorService executor;

  @Before
  public void setUp() {
    executor = Executors.newFixedThreadPool(2 * PAIR_COUNT,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("plugin-broker-%d").build());
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
    // Reset broker state between iterations so a leaked entry from one test method doesn't
    // pollute the next.
    LlapPluginBroker.INSTANCE.clear();
  }

  @Test
  public void testConcurrentPairsCorrectly() throws Exception {
    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      runOneIteration(iteration);
    }
  }

  /**
   * One iteration of the broker stress: build {@link #PAIR_COUNT} plugin pairs concurrently
   * and verify that every communicator ended up bound to the scheduler for its own
   * {@link ApplicationAttemptId}. Runs assertions inline so a failure in any iteration fails the
   * whole test with a specific pair index.
   */
  private void runOneIteration(int iteration) throws Exception {
    List<ApplicationAttemptId> appAttemptIds = new ArrayList<>(PAIR_COUNT);
    for (int p = 0; p < PAIR_COUNT; p++) {
      // A distinct ApplicationId per pair — the (iteration, p) tuple keeps IDs unique across
      // iterations too, so a stale entry from a previous iteration can't accidentally match.
      ApplicationId appId = ApplicationId.newInstance(1000000L + iteration, p + 1);
      appAttemptIds.add(ApplicationAttemptId.newInstance(appId, 1));
    }

    // Two synchronization primitives:
    //   * a barrier so all 2N threads reach the plugin ctor together, maximizing the race window,
    //   * a latch so the main thread waits for every ctor to complete.
    final CyclicBarrier barrier = new CyclicBarrier(2 * PAIR_COUNT);
    final CountDownLatch done = new CountDownLatch(2 * PAIR_COUNT);

    List<AtomicReference<LlapTaskCommunicator>> comms = new ArrayList<>(PAIR_COUNT);
    List<AtomicReference<LlapTaskSchedulerService>> schedulers = new ArrayList<>(PAIR_COUNT);
    List<AtomicReference<Throwable>> errors = Collections.synchronizedList(new ArrayList<>());

    for (int p = 0; p < PAIR_COUNT; p++) {
      comms.add(new AtomicReference<>());
      schedulers.add(new AtomicReference<>());
    }

    for (int p = 0; p < PAIR_COUNT; p++) {
      final int idx = p;
      final ApplicationAttemptId appAttemptId = appAttemptIds.get(p);

      executor.submit(() -> {
        try {
          barrier.await();
          comms.get(idx).set(new LlapTaskCommunicatorForBrokerTest(mockCommContext(appAttemptId)));
        } catch (Throwable t) {
          AtomicReference<Throwable> slot = new AtomicReference<>(t);
          errors.add(slot);
        } finally {
          done.countDown();
        }
      });

      executor.submit(() -> {
        try {
          barrier.await();
          schedulers.get(idx).set(new LlapTaskSchedulerServiceForBrokerTest(mockSchedulerContext(appAttemptId)));
        } catch (Throwable t) {
          AtomicReference<Throwable> slot = new AtomicReference<>(t);
          errors.add(slot);
        } finally {
          done.countDown();
        }
      });
    }

    assertTrue("all ctors must complete within " + AWAIT_TIMEOUT_SECONDS + "s",
        done.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

    if (!errors.isEmpty()) {
      Throwable first = errors.get(0).get();
      throw new AssertionError("iteration " + iteration + " had "
          + errors.size() + " ctor errors; first: " + first, first);
    }

    // Each pair must be internally consistent: comm.scheduler == matching scheduler,
    // scheduler's paired communicator == matching comm.
    for (int p = 0; p < PAIR_COUNT; p++) {
      LlapTaskCommunicator c = comms.get(p).get();
      LlapTaskSchedulerService s = schedulers.get(p).get();
      assertSame("iteration " + iteration + " pair " + p + ": comm.scheduler must be its own peer",
          s, c.getScheduler());
      assertSame("iteration " + iteration + " pair " + p + ": scheduler.communicator must be its own peer",
          c, s.getTaskCommunicator());
    }

    // After every pair has been brokered, both maps must be empty — pairing removes the parked
    // side, and shutdown() reaps orphans. Leftover entries would indicate either a cross-pair
    // mis-pairing (which the assertions above already catch) or a leak.
    assertTrue("pendingCommunicators must drain",
        LlapPluginBroker.INSTANCE.pendingCommunicatorsView().isEmpty());
    assertTrue("pendingSchedulers must drain",
        LlapPluginBroker.INSTANCE.pendingSchedulersView().isEmpty());
  }

  /**
   * Verifies the {@code shutdown()} reap: park a plugin without its peer ever arriving, then
   * call {@code shutdown()} — the map must be empty afterwards.
   */
  @Test
  public void testShutdownReapsOrphanedEntry() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(200000L, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);

    LlapTaskCommunicator comm =
        new LlapTaskCommunicatorForBrokerTest(mockCommContext(appAttemptId));
    assertSame("comm parks itself when peer scheduler is absent", comm,
        LlapPluginBroker.INSTANCE.pendingCommunicatorsView().get(appAttemptId));

    comm.shutdown();
    assertFalse("shutdown() must reap our parked entry",
        LlapPluginBroker.INSTANCE.pendingCommunicatorsView().containsKey(appAttemptId));

    // Symmetric case for the scheduler.
    LlapTaskSchedulerService scheduler =
        new LlapTaskSchedulerServiceForBrokerTest(mockSchedulerContext(appAttemptId));
    assertSame("scheduler parks itself when peer comm is absent", scheduler,
        LlapPluginBroker.INSTANCE.pendingSchedulersView().get(appAttemptId));

    scheduler.shutdown();
    assertFalse("shutdown() must reap our parked entry",
        LlapPluginBroker.INSTANCE.pendingSchedulersView().containsKey(appAttemptId));
  }

  private static TaskCommunicatorContext mockCommContext(ApplicationAttemptId appAttemptId)
      throws Exception {
    TaskCommunicatorContext ctx = mock(TaskCommunicatorContext.class);
    doReturn(appAttemptId).when(ctx).getApplicationAttemptId();
    doReturn(new Credentials()).when(ctx).getAMCredentials();
    Configuration conf = new Configuration(false);
    HiveConf.setVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "fake-non-zk-cluster");
    doReturn(TezUtils.createUserPayloadFromConf(conf)).when(ctx).getInitialUserPayload();
    doReturn(appAttemptId.getApplicationId().toString()).when(ctx).getCurrentAppIdentifier();
    doReturn(mock(DagInfo.class)).when(ctx).getCurrentDagInfo();
    doReturn(new ArrayList<String>()).when(ctx).getInputVertexNames(org.mockito.ArgumentMatchers.any());
    return ctx;
  }

  private static TaskSchedulerContext mockSchedulerContext(ApplicationAttemptId appAttemptId)
      throws Exception {
    TaskSchedulerContext ctx = mock(TaskSchedulerContext.class);
    doReturn(appAttemptId).when(ctx).getApplicationAttemptId();
    doReturn(11111L).when(ctx).getCustomClusterIdentifier();
    Configuration conf = new Configuration(false);
    HiveConf.setVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "fake-non-zk-cluster");
    HiveConf.setVar(conf, ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME, "");
    doReturn(TezUtils.createUserPayloadFromConf(conf)).when(ctx).getInitialUserPayload();
    return ctx;
  }

  /**
   * Minimal LlapTaskCommunicator subclass that skips RPC server startup so we can spin up many
   * of them in a single JVM without port collisions.
   */
  private static final class LlapTaskCommunicatorForBrokerTest extends LlapTaskCommunicator {
    LlapTaskCommunicatorForBrokerTest(TaskCommunicatorContext ctx) {
      super(ctx);
    }
    @Override
    protected void startRpcServer() {
      // no-op — we do not exercise task submission in this test
    }
  }

  /**
   * Minimal LlapTaskSchedulerService subclass — the base ctor does the broker registration,
   * which is all we need.
   */
  private static final class LlapTaskSchedulerServiceForBrokerTest
      extends LlapTaskSchedulerService {
    LlapTaskSchedulerServiceForBrokerTest(TaskSchedulerContext ctx) {
      super(ctx, new org.apache.hadoop.yarn.util.MonotonicClock(), false);
    }
  }
}
