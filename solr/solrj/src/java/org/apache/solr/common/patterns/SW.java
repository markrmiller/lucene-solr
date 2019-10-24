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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.CloseTimeTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.util.OrderedExecutor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DoSmartWork. A utility class that tries to use good patterns, parallelism, logging and tracking.
 */
@SuppressWarnings("serial")
public class SW implements Closeable {

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
    }
  }

  private List<WorkUnit> workUnits = new ArrayList<>();

  private final CloseTimeTracker tracker;

  private boolean closeExecutor;
  
  // nocommit should take logger as well
  public static class Exp extends SolrException {

    public Exp(Throwable th) {
      this(ErrorCode.SERVER_ERROR, null, th);
    }
    
    public Exp(String msg, Throwable th) {
      this(ErrorCode.SERVER_ERROR, msg, th);
    }
    
    public Exp(ErrorCode code, String msg, Throwable th) {
      super(code, msg == null ? "Hit an Exception" : msg, th);
      log.error("Hit an Exception", th);
      if (th instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (th instanceof KeeperException) { // TODO maybe start using ZooKeeperException
        if (((KeeperException) th).code() == KeeperException.Code.SESSIONEXPIRED) {
          log.warn("The session has expired, give up any leadership roles!");
        }
      }
    }

  }

  public SW(Object object) {

   tracker = new CloseTimeTracker(object, object.getClass().getName());

    // constructor must stay very light weight
  }

  public void collect(Object object) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    
    collectSet.add(object);
  }
  
  public void collect(Callable<?> object) {
    collectSet.add(object);
  }
  
  public void collect(Runnable object) {
    collectSet.add(object);
  }
  
  public void addCollect(String label) {
    add(label, collectSet);
    collectSet.clear();
  }

  // add a unit of work
  @SuppressWarnings("unchecked")
  public void add(String label, Object... objects) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object objects={}) - start", label, objects);
    }

    ArrayList<Object> objectList = new ArrayList<>();

    if (objects != null) {
      for (Object object : objects) {
        if (object == null) continue;
        if (object instanceof Collection) {
          objectList.addAll((Collection<? extends Object>) object);
        } else {
          objectList.add(object);
        }
      }
    }

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

  public void add(String label, Callable<?>... callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Callable<?> callables={}) - start", label, callables);
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

  public void add(String label, Object object, Callable Callable) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object={}, Callable Callable={}) - start", label, object, Callable);
    }

    List<Object> objects = new ArrayList<>();
    objects.add(Callable);

    gatherObjects(object, objects);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object, Callable) - end");
    }
  }

  private void gatherObjects(Object object, List<Object> objects) {
    if (log.isDebugEnabled()) {
      log.debug("gatherObjects(Object object={}, List<Object> objects={}) - start", object, objects);
    }

    if (object != null) {
      if (object instanceof Collection) {
        objects.addAll((Collection<? extends Object>) object);
      } else if (object instanceof Map<?,?>) {
        objects.addAll(((Map) object).keySet());
      } else {
        objects.add(object);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("gatherObjects(Object, List<Object>) - end");
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

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object, Callable) - end");
    }
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
      log.debug("add(String, Object, Object, Callable<?>) - end");
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

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object, Object, Object, Callable<?>) - end");
    }
  }

  public void add(String label, List<Callable<?>> Callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, List<Callable<?>> Callables={}) - start", label, Callables);
    }

    List<Object> objects = new ArrayList<>();
    objects.addAll(Callables);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, List<Callable<?>>) - end");
    }
  }

  @Override
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    ExecutorService exec = threadLocal.get();

    if (exec == null) {
      if (log.isDebugEnabled()) {
        log.debug("Starting a new executor");
      }
      executor = new MDCAwareThreadPoolExecutor(0, Runtime.getRuntime().availableProcessors(),
          5L, TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          new SolrjNamedThreadFactory("doSmartWork"));
      closeExecutor = true;
      threadLocal.set(executor);
    } else {
      if (log.isDebugEnabled()) {
        log.debug("Reusing a parent executor");
      }
      executor = exec;
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

          List<Callable<Object>> closeCalls = new ArrayList<Callable<Object>>();

          for (Object object : objects) {

            if (object == null) continue;

            if (objects.size() == 1) {
              handleObject(exception, workUnitTracker, closeCalls, object);
            } else {
              closeCalls.add(() -> {
                handleObject(exception, workUnitTracker, closeCalls, object);
                return object;
              });
            }
          }
          if (closeCalls.size() > 0) {
            try {
              executor.invokeAll(closeCalls);
            } catch (InterruptedException e1) {
              log.error("close()", e1);

              Thread.currentThread().interrupt();
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

      if (closeExecutor) {
        threadLocal.set(null);
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }

      tracker.doneClose();

      if (exception.get() != null) {
        Throwable exp = exception.get();
        if (exp instanceof RuntimeException) {
          throw (RuntimeException) exp;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, exp);
        }
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  private void handleObject(AtomicReference<Throwable> exception, final CloseTimeTracker workUnitTracker,
      List<Callable<Object>> closeCalls, Object object) {
    if (log.isDebugEnabled()) {
      log.debug("handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, List<Callable<Object>> closeCalls={}, Object object={}) - start", exception, workUnitTracker, closeCalls, object);
    }

    Object returnObject = null;
    CloseTimeTracker subTracker = workUnitTracker.startSubClose(object);
    try {
      boolean handled = false;
      if (object instanceof ExecutorService) {
        ExecutorUtil.shutdownAndAwaitTermination((ExecutorService) object);
        handled = true;
      }
      if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
        handled = true;
      }
      if (object instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) object);
        handled = true;
      }
      if (object instanceof AutoCloseable) {
        IOUtils.closeQuietly((AutoCloseable) object);
        handled = true;
      }
      if (object instanceof Callable) {
        returnObject = ((Callable<?>) object).call();
        handled = true;
      }

      if (object instanceof Runnable) {
        ((Runnable) object).run();

        handled = true;
      }

      if (!handled) {
        IllegalArgumentException illegal = new IllegalArgumentException(
            "I do not know how to close: " + object.getClass().getName());
        exception.set(illegal);
      }
    } catch (Exception e) {
      log.error("handleObject(AtomicReference<Throwable>=" + exception + ", CloseTimeTracker=" + workUnitTracker + ", List<Callable<Object>>=" + closeCalls + ", Object=" + object + ")", e);

      throw new RuntimeException(e);
    } finally {
      subTracker.doneClose(returnObject instanceof String ? (String) returnObject
          : (returnObject == null ? "" : returnObject.getClass().getName()));
    }

    if (log.isDebugEnabled()) {
      log.debug("handleObject(AtomicReference<Throwable>, CloseTimeTracker, List<Callable<Object>>, Object) - end");
    }
  }

}
