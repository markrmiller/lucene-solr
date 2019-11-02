/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.patterns;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CloseTimeTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.util.OrderedExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DoWork. A workhorse utility class that tries to use good patterns, parallelism, logging and tracking.
 * 
 * You can give it Closeable objects, executors, Runnables, Callables, Maps, or Collections and it will either run them, close them, or shut them down.
 * Everything in an add call is executed in parralel with each add happening in order. Execution happens on DW#close.
 * 
 * You can also use collect and addCollect to build up a Collection of the above objects and then execute it like a single add.
 * 
 * DW$Exp handles exceptions properly in most contexts.
 * 
 */
@SolrSingleThreaded
public class DW implements Closeable {

  private static final String SOLR_RAN_INTO_AN_ERROR_WHILE_DOING_WORK = "Solr ran into an error while doing work!";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final static ThreadLocal<ExecutorService> threadLocal = new ThreadLocal<>();

  private ExecutorService executor;
  
  private Set<Object> collectSet = null;

  private static class WorkUnit {
    private final List<Object> objects;
    private final CloseTimeTracker tracker;
    private final String label;

    public WorkUnit(List<Object> objects, CloseTimeTracker tracker, String label) {
      this.objects = objects;
      this.tracker = tracker;
      this.label = label;
      
      assert checkTypesForTests(objects);
    }

    private boolean checkTypesForTests(List<Object> objects) {
      for (Object object : objects) {
        assert !(object instanceof Collection);
        assert !(object instanceof Map);
        assert !(object.getClass().isArray());
      }
      
      return true;
    }
  }

  private List<WorkUnit> workUnits = new ArrayList<>();

  private final CloseTimeTracker tracker;

  private final boolean ignoreExceptions;

  private Set<Throwable> warns = DW.concSetSmallO();

  // nocommit should take logger as well
  public static class Exp extends SolrException {

    private static final String ERROR_MSG = "Solr ran into an unexpected Exception";

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     */
    public Exp(String msg) {
      this(ErrorCode.SERVER_ERROR, msg, null);
    }
    
    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param th the exception to handle
     */
    public Exp(Throwable th) {
      this(ErrorCode.SERVER_ERROR, th.getMessage(), th);
    }
    

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     * @param th the exception to handle
     */
    public Exp(String msg, Throwable th) {
      this(ErrorCode.SERVER_ERROR, msg, th);
    }
    
    public Exp(ErrorCode code, String msg, Throwable th) {
      super(code, msg == null ? ERROR_MSG : msg, th);
      log.error(ERROR_MSG, th);
      if (th != null && th instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (th != null && th instanceof KeeperException) { // TODO maybe start using ZooKeeperException
        if (((KeeperException) th).code() == KeeperException.Code.SESSIONEXPIRED) {
          log.warn("The session has expired, give up any leadership roles!");
        }
      }
    }
  }

  public DW(Object object) {
    this(object, false);
  }

  public DW(Object object, boolean ignoreExceptions) {
    this.ignoreExceptions = ignoreExceptions;
    tracker = new CloseTimeTracker(object, object == null ? "NullObject" : object.getClass().getName());
    // constructor must stay very light weight
  }

  public void collect(Object object) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    
    collectSet.add(object);
  }
  
  
  /**
   * @param callable A Callable to run. If an object is return, it's toString is used to identify it.
   */
  public void collect(Callable<?> callable) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    collectSet.add(callable);
  }
  
  /**
   * @param runnable A Runnable to run. If an object is return, it's toString is used to identify it.
   */
  public void collect(Runnable runnable) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    collectSet.add(runnable);
  }
  
  public void addCollect(String label) { 
    if (collectSet == null) {
      log.info("Nothing collected to submit");
      return;
    }
    add(label, collectSet);
    collectSet.clear();
  }

  // add a unit of work
  public void add(String label, Object... objects) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object objects={}) - start", label, objects);
    }

    List<Object> objectList = new ArrayList<>();

    gatherObjects(objects, objectList);

    WorkUnit workUnit = new WorkUnit(objectList, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object) - end");
    }
  }

  public void add(Object object) {
    if (log.isDebugEnabled()) {
      log.debug("add(Object object={}) - start", object);
    }

    if (object == null) return;
    if (object instanceof Collection<?>) {
      throw new IllegalArgumentException("Use this method only with a single Object");
    }
    add(object.getClass().getName(), object);

    if (log.isDebugEnabled()) {
      log.debug("add(Object) - end");
    }
  }

  public void add(String label, Callable<?> callable) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, callable<?> callable={}) - start", label, callable);
    }
    WorkUnit workUnit = new WorkUnit(Collections.singletonList(callable), tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, callable<?>) - end");
    }
  }
  
  public void add(String label, Callable<?>... callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, callable<?> callables={}) - start", label, callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(callables));

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Callable<?>) - end");
    }
  }

  public void add(String label, Runnable... tasks) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Runnable tasks={}) - start", label, tasks);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(tasks));

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Runnable) - end");
    }
  }

  
  
  /**
   * () -> {
   *     obj.shutdown();
   *     return obj;
   *   });
   * 
   * Runs the callable and closes the object in parallel. The object return will
   * be used for tracking. You can return a String if you prefer.
   * 
   */
  public void add(String label, Object object, Callable callable) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object={}, Callable Callable={}) - start", label, object, callable);
    }

    List<Object> objects = new ArrayList<>();
    objects.add(callable);

    gatherObjects(object, objects);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object, Callable) - end");
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void gatherObjects(Object object, List<Object> objects) {
    if (log.isDebugEnabled()) {
      log.debug("gatherObjects(Object object={}, List<Object> objects={}) - start", object, objects);
    }

    if (object != null) {
      if (object.getClass().isArray()) {
        if (log.isDebugEnabled()) {
          log.debug("Found an array to gather against");
        }

        for (Object obj : (Object[]) object) {
          gatherObjects(obj, objects);
        }

      } else if (object instanceof Collection) {
        if (log.isDebugEnabled()) {
          log.debug("Found a Collectiom to gather against");
        }
        for (Object obj : (Collection)object) {
          gatherObjects(obj, objects);
        }
      } else if (object instanceof Map<?,?>) {
        if (log.isDebugEnabled()) {
          log.debug("Found a Map to gather against");
        }
        ((Map) object).forEach((k, v) -> gatherObjects(v, objects));
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Found a non collection object to add {}", object.getClass().getName());
        }
        objects.add(object);
      }
    }
  }

  public void add(String label, Object object, Callable... Callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object={}, Callable Callables={}) - start", label, object, Callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));
    gatherObjects(object, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object1, Object object2, Callable<?>... Callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object1={}, Object object2={}, Callable<?> Callables={}) - start", label, object1, object2, Callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));

    gatherObjects(object1, objects);
    gatherObjects(object2, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
    if (log.isDebugEnabled()) {
      log.debug("Add WorkUnit:" + objects); // nocommit
    }
  }

  public void add(String label, Object object1, Object object2, Object object3, Callable<?>... Callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object1={}, Object object2={}, Object object3={}, Callable<?> Callables={}) - start", label, object1, object2, object3, Callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));
    gatherObjects(object1, objects);
    gatherObjects(object2, objects);
    gatherObjects(object3, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, List<Callable<?>> Callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, List<Callable<?>> Callables={}) - start", label, Callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Callables);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  @Override
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }
    
    if (collectSet != null && collectSet.size() > 1) {
      throw new IllegalStateException("addCollect must be called to add any objects collected!");
    }
    
    AtomicReference<Throwable> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {

        final CloseTimeTracker workUnitTracker = workUnit.tracker.startSubClose(workUnit.label);
        try {
          List<Object> objects = workUnit.objects;

          if (objects.size() == 1) {
            handleObject(workUnit.label, exception, workUnitTracker, Collections.emptyList(), objects.get(0));
          } else {

            List<Callable<Object>> closeCalls = new ArrayList<Callable<Object>>();

            for (Object object : objects) {

              if (object == null) continue;

              initExecutor();

              closeCalls.add(() -> {
                handleObject(workUnit.label, exception, workUnitTracker, closeCalls, object);
                return object;
              });

            }
            if (closeCalls.size() > 0) {
              try {
                executor.invokeAll(closeCalls);
              } catch (InterruptedException e1) {
                log.error("close()", e1);
                Thread.currentThread().interrupt();
                throw new RuntimeException("Close was interrupted!");
              }
            }
          }
        } finally {
          if (workUnitTracker != null) workUnitTracker.doneClose();
        }

      }
    } catch (Throwable t) {
      log.error("close()", t);

      if (t instanceof Error) {
        throw (Error) t;
      }
      exception.set(t);
    } finally {

      tracker.doneClose();

      warns.forEach(
          (it) -> log.warn(SOLR_RAN_INTO_AN_ERROR_WHILE_DOING_WORK, new SolrException(ErrorCode.SERVER_ERROR, it)));

      if (exception.get() != null) {
        Throwable exp = exception.get();
        throw new SolrException(ErrorCode.SERVER_ERROR, exp);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  private void initExecutor() {
    if (executor != null) return;
    
    ExecutorService exec = threadLocal.get();

    if (exec == null) {
      if (log.isDebugEnabled()) {
        log.debug("Starting a new executor");
      }
      
      executor = new MDCAwareThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors(),
          5L, TimeUnit.SECONDS,
          new SynchronousQueue<>(), new SolrjNamedThreadFactory("Solr-DoWork", true, 7));
      
      threadLocal.set(executor);
    } else {
      if (log.isDebugEnabled()) {
        log.debug("Reusing a parent executor");
      }
      executor = exec;
    }
  }

  private void handleObject(String label, AtomicReference<Throwable> exception, final CloseTimeTracker workUnitTracker,
      List<Callable<Object>> closeCalls, Object object) {
    if (log.isDebugEnabled()) {
      log.debug(
          "handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, List<Callable<Object>> closeCalls={}, Object object={}) - start",
          exception, workUnitTracker, closeCalls, object);
    }
    
    if (object != null) {
      assert !(object instanceof Collection);
      assert !(object instanceof Map);
      assert !(object.getClass().isArray());
    }

    Object returnObject = null;
    CloseTimeTracker subTracker = workUnitTracker.startSubClose(object);
    try {
      boolean handled = false;
      if (object instanceof ExecutorService) {
        ExecutorUtil.shutdownAndAwaitTermination((ExecutorService) object);
        handled = true;
      } else if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
        handled = true;
      } else if (object instanceof Closeable) {
        ((Closeable) object).close();
        handled = true;
      } else if (object instanceof AutoCloseable) {
        ((AutoCloseable) object).close();
        handled = true;
      }else if (object instanceof Callable) {
        returnObject = ((Callable<?>) object).call();
        handled = true;
      } else if (object instanceof Runnable) {
        ((Runnable) object).run();

        handled = true;
      } else if (object instanceof Timer) {
        ((Timer) object).cancel();
        
        handled = true;
      }
      
      if (!handled) {
        IllegalArgumentException illegal = new IllegalArgumentException(label + " -> I do not know how to close: " + object.getClass().getName());
        exception.set(illegal);
      }
    } catch (Throwable t) {

      if (t instanceof NullPointerException) {
        log.info("NPE closing " + object == null ? "Null Object" : object.getClass().getName());
      } else {
        if (ignoreExceptions) {
          warns .add(t);
          if (t instanceof Error) {
            throw (Error) t;
          }
        } else {

          log.error("handleObject(AtomicReference<Throwable>=" + exception + ", CloseTimeTracker=" + workUnitTracker
              + ", List<Callable<Object>>=" + closeCalls + ", Object=" + object + ")", t);
          propegateInterrupt(t);
          if (t instanceof Error) {
            throw (Error) t;
          }
          throw new SolrException(ErrorCode.SERVER_ERROR, SOLR_RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);
        }
      }
    } finally {
      subTracker.doneClose(returnObject instanceof String ? (String) returnObject
          : (returnObject == null ? "" : returnObject.getClass().getName()));
    }

    if (log.isDebugEnabled()) {
      log.debug("handleObject(AtomicReference<Throwable>, CloseTimeTracker, List<Callable<Object>>, Object) - end");
    }
  }
  
  /**
   * Sugar method to close objects.
   * 
   * @param object to close
   */
  public static void close(Object object, boolean ignoreExceptions) {
    try (DW dw = new DW(object, ignoreExceptions)) {
      dw.add(object);
    }
  }
  
  public static void close(Object object) {
    try (DW dw = new DW(object)) {
      dw.add(object);
    }
  }

  public static <K> Set<K> concSetSmallO() {
    return ConcurrentHashMap.newKeySet(50);
  }
  
  public static <K,V> ConcurrentHashMap<K,V> concMapSmallO() {
    return new ConcurrentHashMap<K,V>(132, 0.75f, 50);
  }
  
  public static ConcurrentHashMap<?,?> concReqsO() {
    return new ConcurrentHashMap<>(128, 0.75f, 2048);
  }
  
  public static ConcurrentHashMap<?,?> concMapClassesO() {
    return new ConcurrentHashMap<>(132, 0.75f, 8192);
  }

  public static void propegateInterrupt(Throwable t) {
    propegateInterrupt(t, false);
  }
  
  public static void propegateInterrupt(Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(t.getMessage());
      } else {
        log.warn("Solr ran into an unexpected exception", t);
      }
    }
    
    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void propegateInterrupt(String msg, Throwable t) {
    propegateInterrupt(msg, t, false);
  }
  
  public static void propegateInterrupt(String msg, Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(msg);
      } else {
        log.warn(msg, t);
      }
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
  }
  
  
  // think about second class
  public static void waitForExists(SolrZkClient zkClient, String znodePath) {

    CountDownLatch latch = new CountDownLatch(1);

    // nocommit TODO one live node watcher, but above zkstatereader
    Stat stat = null;
    try {
      stat = zkClient.getCurator().checkExists().usingWatcher((CuratorWatcher) event -> {

        log.info("Got event on live node watcher {}", event.toString());
        if (event.getType() == EventType.NodeCreated) {
          if (zkClient.getCurator().checkExists().forPath(znodePath) != null) {
            latch.countDown();
          }
        }

      }).forPath(znodePath);
    } catch (Exception e) {
      throw new DW.Exp(e);
    }
    if (stat == null) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        DW.propegateInterrupt(e);
      }
    }

  }

}
