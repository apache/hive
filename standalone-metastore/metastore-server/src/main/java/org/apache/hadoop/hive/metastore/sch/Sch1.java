package org.apache.hadoop.hive.metastore.sch;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Optional;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

// FIXME remove
public class Sch1 {

  public static void main(String[] args) {
    CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    // Create a parser based on provided definition
    CronParser parser = new CronParser(cronDefinition);
    //    Cron quartzCron = parser.parse("0 23 * ? * 1-5 *");

    // Get date for last execution
    ZonedDateTime now = ZonedDateTime.now();
    ExecutionTime executionTime = ExecutionTime.forCron(parser.parse("47 6  * * * "));
    Optional<ZonedDateTime> lastExecution = executionTime.lastExecution(now);
    System.out.println(lastExecution);

    long es = lastExecution.get().toEpochSecond();
    Instant ii = Instant.ofEpochSecond(es);
    ZonedDateTime z2 = ZonedDateTime.ofInstant(ii, lastExecution.get().getZone());

    // Get date for next execution
    Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(z2);
    System.out.println(nextExecution);

  }

}
