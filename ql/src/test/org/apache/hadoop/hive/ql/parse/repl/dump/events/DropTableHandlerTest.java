package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DropTableHandlerTest {

  @Mock
  private EventHandler.Context context;

  @Test
  public void tableNameAndDatabaseNameIsLowerCase() throws Exception {
    DropTableMessage dropTableMessage =
        MessageFactory.getInstance().buildDropTableMessage(
            new Table("SomeTable", "InADB", "", Integer.MAX_VALUE, Integer.MAX_VALUE,
                Integer.MAX_VALUE, new StorageDescriptor(), new ArrayList<>(), new HashMap<>(), "",
                "", ""));

    DropTableHandler handler =
        new DropTableHandler(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            EventMessage.EventType.DROP_TABLE.toString(), dropTableMessage.toString()));

    DumpMetaData mockDmd = mock(DumpMetaData.class);
    when(context.createDmd(anyObject())).thenReturn(mockDmd);

    handler.handle(context);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockDmd).setPayload(captor.capture());
    String value = captor.getValue();
    assertTrue(value + " does not contain dbName [inadb] in lower case", value.contains("inadb"));
    assertTrue(value + " does not contain tableName [sometable] in lower case",
        value.contains("sometable"));
  }
}