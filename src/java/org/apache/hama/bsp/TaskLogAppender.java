package org.apache.hama.bsp;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.FileAppender;
import org.apache.log4j.spi.LoggingEvent;

public class TaskLogAppender extends FileAppender {
  private String taskId; // taskId should be managed as String rather than
                         // TaskID object
  // so that log4j can configure it from the configuration(log4j.properties).
  private int maxEvents;
  private Queue<LoggingEvent> tail = null;

  @Override
  public void activateOptions() {
    synchronized (this) {
      if (maxEvents > 0) {
        tail = new LinkedList<LoggingEvent>();
      }
      setFile(TaskLog.getTaskLogFile(TaskAttemptID.forName(taskId),
          TaskLog.LogName.SYSLOG).toString());
      setAppend(true);
      super.activateOptions();
    }
  }

  @Override
  public void append(LoggingEvent event) {
    synchronized (this) {
      if (tail == null) {
        super.append(event);
      } else {
        if (tail.size() >= maxEvents) {
          tail.remove();
        }
        tail.add(event);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (tail != null) {
      for (LoggingEvent event : tail) {
        super.append(event);
      }
    }
    super.close();
  }

  /**
   * Getter/Setter methods for log4j.
   */

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  private static final int EVENT_SIZE = 100;

  public long getTotalLogFileSize() {
    return maxEvents * EVENT_SIZE;
  }

  public void setTotalLogFileSize(long logSize) {
    maxEvents = (int) logSize / EVENT_SIZE;
  }

}
