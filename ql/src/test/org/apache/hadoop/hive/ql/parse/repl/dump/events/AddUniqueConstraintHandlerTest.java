package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddUniqueConstraintMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AddUniqueConstraintHandlerTest {

  @Mock
  private EventHandler.Context context;

  @Test
  public void tableNameAndDatabaseNameIsLowerCase() throws Exception {
    SQLUniqueConstraint constraint =
        new SQLUniqueConstraint("TABLE_NAME", "DB_NAME", "col1", Integer.MAX_VALUE, "pk", true,
            true,
            true);

    AddUniqueConstraintMessage message = MessageFactory.getInstance()
        .buildAddUniqueConstraintMessage(Collections.singletonList(constraint));

    AddUniqueConstraintHandler handler =
        new AddUniqueConstraintHandler(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            EventMessage.EventType.ADD_UNIQUECONSTRAINT.toString(), message.toString())) {
          @Override
          boolean shouldReplicate(Context withinContext) {
            return true;
          }
        };

    DumpMetaData mockDmd = mock(DumpMetaData.class);
    when(context.createDmd(anyObject())).thenReturn(mockDmd);

    handler.handle(context);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockDmd).setPayload(captor.capture());
    String value = captor.getValue();
    assertTrue(value + " does not contain dbName [db_name] in lower case",
        value.contains("db_name"));
    assertTrue(value + " does not contain tableName [table_name] in lower case",
        value.contains("table_name"));
  }
}