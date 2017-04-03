package org.apache.hadoop.hive.ql.parse.repl.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestEventHandlerFactory {
  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowRegisteringEventsWhichCannotBeInstantiated() {
    class NonCompatibleEventHandler implements EventHandler {
      @Override
      public void handle(Context withinContext) throws Exception {

      }

      @Override
      public long fromEventId() {
        return 0;
      }

      @Override
      public long toEventId() {
        return 0;
      }

      @Override
      public ReplicationSemanticAnalyzer.DUMPTYPE dumpType() {
        return null;
      }
    }
    EventHandlerFactory.register("anyEvent", NonCompatibleEventHandler.class);
  }

  @Test
  public void shouldProvideDefaultHandlerWhenNothingRegisteredForThatEvent() {
    EventHandler eventHandler =
        EventHandlerFactory.handlerFor(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            "shouldGiveDefaultHandler", "s"));
    assertTrue(eventHandler instanceof DefaultHandler);
  }

}