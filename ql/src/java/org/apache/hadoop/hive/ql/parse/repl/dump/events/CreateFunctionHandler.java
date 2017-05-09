package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

class CreateFunctionHandler extends AbstractEventHandler {
  CreateFunctionHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    CreateFunctionMessage createFunctionMessage =
        deserializer.getCreateFunctionMessage(event.getMessage());
    LOG.info("Processing#{} CREATE_MESSAGE message : {}", fromEventId(), event.getMessage());
    Path metadataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    FileSystem fileSystem = metadataPath.getFileSystem(withinContext.hiveConf);

    try (JsonWriter jsonWriter = new JsonWriter(fileSystem, metadataPath)) {
      new FunctionSerializer(createFunctionMessage.getFunctionObj())
          .writeTo(jsonWriter, withinContext.replicationSpec);
    }
    withinContext.createDmd(this).write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_CREATE_FUNCTION;
  }
}
