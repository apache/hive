package org.apache.hive.spark.client;

import org.apache.hive.spark.client.rpc.RpcServer;
import org.apache.spark.launcher.SparkAppHandle;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSparkLauncherSparkClient {

  @Test
  public void testSparkLauncherFutureGet() {
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.SUBMITTED,
            SparkAppHandle.State.RUNNING);
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.SUBMITTED,
            SparkAppHandle.State.FINISHED);
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.SUBMITTED,
            SparkAppHandle.State.FAILED);
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.SUBMITTED,
            SparkAppHandle.State.KILLED);

    testChainOfStates(SparkAppHandle.State.LOST);
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.LOST);
    testChainOfStates(SparkAppHandle.State.CONNECTED, SparkAppHandle.State.SUBMITTED,
            SparkAppHandle.State.LOST);
  }

  private void testChainOfStates(SparkAppHandle.State... states) {
    SparkAppHandle sparkAppHandle = mock(SparkAppHandle.class);
    RpcServer rpcServer = mock(RpcServer.class);
    String clientId = "";

    CountDownLatch shutdownLatch = new CountDownLatch(1);

    SparkLauncherSparkClient.SparkAppListener sparkAppListener = new SparkLauncherSparkClient.SparkAppListener(
            shutdownLatch, rpcServer, clientId);
    Future<Void> sparkLauncherFuture = SparkLauncherSparkClient.createSparkLauncherFuture(
            shutdownLatch, sparkAppHandle, rpcServer, clientId);

    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      try {
        sparkLauncherFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    for (int i = 0; i < states.length - 1; i++) {
      when(sparkAppHandle.getState()).thenReturn(states[i]);
      sparkAppListener.stateChanged(sparkAppHandle);
      Assert.assertTrue(!future.isDone());
    }

    when(sparkAppHandle.getState()).thenReturn(states[states.length - 1]);
    sparkAppListener.stateChanged(sparkAppHandle);
    try {
      future.get(60, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException("SparkLauncherFuture failed to complete after transitioning to " +
              "state " + states[states.length - 1], e);
    }
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(shutdownLatch.getCount(), 0);
    verify(sparkAppHandle).disconnect();
  }
}
