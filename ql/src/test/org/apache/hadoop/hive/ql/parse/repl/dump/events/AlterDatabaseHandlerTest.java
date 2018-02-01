package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AlterDatabaseHandlerTest {

  @Mock
  private EventHandler.Context context;

  @Test
  public void databaseNameIsLowerCase() throws Exception {
    AlterDatabaseMessage alterDatabaseMessage =
        MessageFactory.getInstance().buildAlterDatabaseMessage(
            new Database("BEFORE", "", "", new HashMap<>()),
            new Database("AFTER", "", "", new HashMap<>()));

    AlterDatabaseHandler handler =
        new AlterDatabaseHandler(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            EventMessage.EventType.ALTER_DATABASE.toString(), alterDatabaseMessage.toString()));

    DumpMetaData mockDmd = mock(DumpMetaData.class);
    when(context.createDmd(anyObject())).thenReturn(mockDmd);

    handler.handle(context);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockDmd).setPayload(captor.capture());
    String value = captor.getValue();
    assertTrue(value + " does not contain dbName [before] in lower case", value.contains("before"));
    assertTrue(value + " does not contain dbName [after] in lower case", value.contains("after"));
  }
}