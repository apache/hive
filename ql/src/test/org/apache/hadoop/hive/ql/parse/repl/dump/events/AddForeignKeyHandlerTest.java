package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
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
public class AddForeignKeyHandlerTest {

  @Mock
  private EventHandler.Context context;

  @Test
  public void tableNameAndDatabaseNameIsLowerCase() throws Exception {
    SQLForeignKey sqlForeignKey = new SQLForeignKey(
        "PKDB", "pkTable", "pkcol",
        "FKdb", "FKtABLE", "fk_col",
        Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
        "fkname", "pkname",
        false, false, false);

    AddForeignKeyMessage addForeignKeyMessage = MessageFactory.getInstance()
        .buildAddForeignKeyMessage(Collections.singletonList(sqlForeignKey));

    AddForeignKeyHandler handler =
        new AddForeignKeyHandler(new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
            EventMessage.EventType.ADD_FOREIGNKEY.toString(), addForeignKeyMessage.toString())) {
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
    assertTrue(value + " does not contain pk dbName [pkdb] in lower case", value.contains("pkdb"));
    assertTrue(value + " does not contain pk tableName [pktable] in lower case",
        value.contains("pktable"));
    assertTrue(value + " does not contain fk dbName [fkdb] in lower case", value.contains("fkdb"));
    assertTrue(value + " does not contain fk tableName [fktable] in lower case",
        value.contains("fktable"));
  }
}