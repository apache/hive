package org.apache.hadoop.hive.shims;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.OptionHandler;

public class HiveEventCounter implements Appender, OptionHandler {
  
  AppenderSkeleton hadoopEventCounter;
  
  public HiveEventCounter() {
    hadoopEventCounter = ShimLoader.getEventCounter();
  }

  @Override
  public void close() {
    hadoopEventCounter.close();
  }

  @Override
  public boolean requiresLayout() {
    return hadoopEventCounter.requiresLayout();
  }

  @Override
  public void addFilter(Filter filter) {
    hadoopEventCounter.addFilter(filter);
  }

  @Override
  public void clearFilters() {
    hadoopEventCounter.clearFilters();
  }

  @Override
  public void doAppend(LoggingEvent event) {
    hadoopEventCounter.doAppend(event);
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return hadoopEventCounter.getErrorHandler();
  }

  @Override
  public Filter getFilter() {
    return hadoopEventCounter.getFilter();
  }

  @Override
  public Layout getLayout() {
    return hadoopEventCounter.getLayout();
  }

  @Override
  public String getName() {
    return hadoopEventCounter.getName();
  }

  @Override
  public void setErrorHandler(ErrorHandler handler) {
    hadoopEventCounter.setErrorHandler(handler);
  }

  @Override
  public void setLayout(Layout layout) {
    hadoopEventCounter.setLayout(layout);
  }

  @Override
  public void setName(String name) {
    hadoopEventCounter.setName(name);
  }

  @Override
  public void activateOptions() {
    hadoopEventCounter.activateOptions();
  }

}
