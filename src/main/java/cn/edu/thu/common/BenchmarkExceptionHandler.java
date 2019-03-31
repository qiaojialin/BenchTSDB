package cn.edu.thu.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkExceptionHandler implements Thread.UncaughtExceptionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkExceptionHandler.class);

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    LOGGER.error("Exception in thread {}-{}", t.getName(), t.getId(), e);
  }

}
