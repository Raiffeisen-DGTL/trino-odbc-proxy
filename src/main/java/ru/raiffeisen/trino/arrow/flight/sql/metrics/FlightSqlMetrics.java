package ru.raiffeisen.trino.arrow.flight.sql.metrics;

import javax.management.*;
import java.lang.management.ManagementFactory;

public final class FlightSqlMetrics {

  public static final StatusCount STATUS_COUNT = new StatusCount();
  public static final QueryCount QUERY_COUNT = new QueryCount();
  public static final RowCount ROW_COUNT = new RowCount();
  public static final ListenerWait LISTENER_WAIT = new ListenerWait();
  public static final BufferMemAllocation BUFFER_MEM_ALLOCATION = new BufferMemAllocation();
  
  public static void registerMetrics() {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mBeanServer.registerMBean(STATUS_COUNT, new ObjectName("arrow.flight.server:type=status"));
      mBeanServer.registerMBean(QUERY_COUNT, new ObjectName("arrow.flight.server:type=query"));
      mBeanServer.registerMBean(ROW_COUNT, new ObjectName("arrow.flight.server:type=row"));
      mBeanServer.registerMBean(LISTENER_WAIT, new ObjectName("arrow.flight.server:type=listener"));
      mBeanServer.registerMBean(BUFFER_MEM_ALLOCATION, new ObjectName("arrow.flight.server:type=buffer"));
    } catch (InstanceAlreadyExistsException |
             MBeanRegistrationException |
             NotCompliantMBeanException |
             MalformedObjectNameException e) {
      throw new RuntimeException("Unable to register custom flight metrics due to following error: ", e);
    }
  }
}
