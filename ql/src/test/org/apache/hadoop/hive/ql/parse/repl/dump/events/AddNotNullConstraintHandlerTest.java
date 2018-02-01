package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
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
public class AddNotNullConstraintHandlerTest {

  @Mock
  private EventHandler.Context context;

  @Test
  public void tableNameAndDatabaseNameIsLowerCase() throws Exception {
    SQLNotNullConstraint constraint = new SQLNotNullConstraint(
        "A_DB", "a_Table", "pkcol", "name",
        false, false, false);

    AddNotNullConstraintMessage constraintMessage = MessageFactory.getInstance()
        .buildAddNotNullConstraintMessage(Collections.singletonList(constraint));

    AddNotNullConstraintHandler handler =
        new AddNotNullConstraintHandler(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            EventMessage.EventType.ADD_NOTNULLCONSTRAINT.toString(),
            constraintMessage.toString())) {
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
    assertTrue(value + " does not contain dbName [a_db] in lower case", value.contains("a_db"));
    assertTrue(value + " does not contain tableName [a_table] in lower case",
        value.contains("a_table"));
  }
}