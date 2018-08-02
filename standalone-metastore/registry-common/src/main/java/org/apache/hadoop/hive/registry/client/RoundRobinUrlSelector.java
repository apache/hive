package org.apache.hadoop.hive.registry.client;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class implements round robin strategy to select a url with the given list of urls.
 */
public class RoundRobinUrlSelector extends AbstractUrlSelector {

  private final AtomicLong currentIndex = new AtomicLong(0L);

  public RoundRobinUrlSelector(String clusterUrl) {
    super(clusterUrl);
  }

  @Override
  public String select() {
    return urls[(int) (currentIndex.getAndIncrement() % urls.length)];
  }

  @Override
  public void urlWithError(String url, Exception e) {
    // can be ignored
  }

}