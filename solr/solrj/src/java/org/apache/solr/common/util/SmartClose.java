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
package org.apache.solr.common.util;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartClose implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  AtomicReference<Exception> exception = new AtomicReference<>();

  public final static ExecutorService closeThreadPool = new MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
      5L, TimeUnit.SECONDS,
      new SynchronousQueue<>(),
      new SolrjNamedThreadFactory("solrSmartCloser"));

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

  private List<WorkUnit> workUnits = Collections.synchronizedList(new ArrayList<>());

  private final CloseTimeTracker tracker;

  public SmartClose(Object closeObject) {
    tracker = new CloseTimeTracker(closeObject);
    log.debug("Start tacking close of :" + closeObject.toString());
  }

  // add a unit of work
  @SuppressWarnings("unchecked")
  public void add(String label, Object... objects) {
    ArrayList<Object> objectList = new ArrayList<>();
    for (Object object : objects) {
      if (object instanceof Collection) {
        objectList.addAll((Collection<? extends Object>) object);
      } else {
        objectList.add(object);
      }
    }

    WorkUnit workUnit = new WorkUnit(objectList, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(Object object) {
    List<Object> objects = new ArrayList<>();

    if (object instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object);
    } else {
      objects.add(object);
    }

    WorkUnit workUnit = new WorkUnit(objects, tracker, " ");
    workUnits.add(workUnit);
  }

  public void add(Callable callable) {
    List<Object> objects = new ArrayList<>();
    objects.add(callable);

    WorkUnit workUnit = new WorkUnit(objects, tracker, " ");
    workUnits.add(workUnit);
  }

  public void add(String label, Callable... Callables) {
    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object, Callable Callable) {
    List<Object> objects = new ArrayList<>();
    objects.add(Callable);

    if (object instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object);
    } else {
      objects.add(object);
    }

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object, Callable... Callables) {
    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));

    if (object instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object);
    } else {
      objects.add(object);
    }

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object1, Object object2, Callable... Callables) {
    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));
    if (object1 instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object1);
    } else {
      objects.add(object1);
    }
    if (object2 instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object2);
    } else {
      objects.add(object2);
    }

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object1, Object object2, Object object3, Callable... Callables) {
    List<Object> objects = new ArrayList<>();
    objects.addAll(Arrays.asList(Callables));
    if (object1 instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object1);
    } else {
      objects.add(object1);
    }
    if (object2 instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object2);
    } else {
      objects.add(object2);
    }
    if (object3 instanceof Collection) {
      objects.addAll((Collection<? extends Object>) object3);
    } else {
      objects.add(object3);
    }

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, List<Callable<?>> Callables) {
    List<Object> objects = new ArrayList<>();
    objects.addAll(Callables);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  @Override
  public void close() {
    AtomicReference<Exception> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {

        final CloseTimeTracker workUnitTracker;
        if (workUnit.objects.size() == 1) {
          workUnitTracker = workUnit.tracker.startSubClose(workUnit.objects.get(0));

        } else {
          workUnitTracker = workUnit.tracker.startSubClose(workUnit.label);
        }

        List<Object> objects = workUnit.objects;

        List<Callable<Object>> closeCalls = new ArrayList<Callable<Object>>();

        for (Object object : objects) {
          if (object == null) continue;
          log.debug("add close call for:" + object);
          closeCalls.add(() -> {
            Object returnObject = null;
            CloseTimeTracker subTracker = workUnitTracker.startSubClose(object);
            try {
              boolean handled = false;
              if (object instanceof ExecutorService) {
                log.debug("shutdown exec service: " + object);
                ExecutorUtil.shutdownAndAwaitTermination((ExecutorService) object);
                handled = true;
              }
              if (object instanceof OrderedExecutor) {
                log.debug("shutdown ordered exec: " + object);
                ((OrderedExecutor) object).shutdownAndAwaitTermination();
                handled = true;
              }
              if (object instanceof Closeable) {
                log.debug("close: " + object);
                IOUtils.closeQuietly((Closeable) object);
                handled = true;
              }
              if (object instanceof AutoCloseable) {
                log.debug("close: " + object);
                IOUtils.closeQuietly((AutoCloseable) object);
                handled = true;
              }
              if (object instanceof Callable) {
                log.debug("callable: " + object);
                returnObject = ((Callable<?>) object).call();
                log.debug("Callable returns " + returnObject);
                handled = true;
              }

              if (object instanceof Runnable) {
                log.debug("runnable: " + object);
                ((Runnable) object).run();

                handled = true;
              }

              if (!handled) {
                IllegalArgumentException illegal = new IllegalArgumentException(
                    "I do not know how to close: " + object.getClass().getName());
                log.error("SmartClose cannot close an object it was given", illegal);
                exception.set(illegal);
                return null;
              }
            } finally {
              subTracker.doneClose(returnObject);
            }
            return null;
          });
        }

        try {
          closeThreadPool.invokeAll(closeCalls);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
        workUnitTracker.doneClose();
      }
    } finally {
      tracker.doneClose();

      if (exception.get() != null) {
        log.error("Error during close", exception.get());
        throw new RuntimeException(exception.get());
      }
    }
  }

}
