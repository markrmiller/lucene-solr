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
package org.apache.lucene.gradle.testFast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.internal.Factory;
import org.gradle.internal.UncheckedException;
import org.gradle.internal.actor.Actor;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.concurrent.CompositeStoppable;
import org.gradle.internal.concurrent.Stoppable;
import org.gradle.internal.dispatch.DispatchException;

public class JVMTestQueues implements Stoppable {
  // private static final Logger log = Logging.getLogger(JVMTestQueues.class);

  private final ActorFactory actorFactory;
  private final List<TestClassProcessor> processors = new ArrayList<TestClassProcessor>();
  private final List<TestClassProcessor> rawProcessors = new ArrayList<TestClassProcessor>();
  private final List<Actor> actors = new ArrayList<Actor>();
  private final Factory<TestClassProcessor> factory;
  private volatile Actor resultProcessorActor;

  private int maxProcs;

  private TestComm testComm;

  public JVMTestQueues(int maxProcessors, TestComm testComm, Factory<TestClassProcessor> factory,
      ActorFactory actorFactory) {
    this.testComm = testComm;
    this.factory = factory;
    this.actorFactory = actorFactory;
    this.maxProcs = maxProcessors;
  }

  private final Map<NamedTestClassProcessor,List<String>> testTracking = new LinkedHashMap<>();

  private final AtomicInteger testsAdded = new AtomicInteger(1);
  
  private AtomicBoolean paused = new AtomicBoolean(false);
  private TestResultProcessor resultProcessor;

  
  public void testDone(Object testId) {
    //testComm.log(getName() + " completed:" + testId + " " + testComm.getTestClasses().getIdToTest().get(testId));
    String test = testComm.getTestClasses().getIdToTest().get(testId);
    //testComm.log(getName() + " " + test);
    if (test == null) return;
    checkQueues(testComm);
    
    try {
      completed(test);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
  

  public TestClasses getTestClasses() {
    return testComm.getTestClasses();
  }

  public void completed(String test) {
 
    if (test == null) {
      return;
    }
    //testComm.log(getName() + " Remove finished test from JVM queue " + test);
    
    synchronized (testTracking) {
      try {
        Collection<List<String>> lists = testTracking.values();
        for (List<String> list : lists) {
          list.remove(test);
        }
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  public void addProcessor(NamedTestClassProcessor processor) {
    if (processor == null) return;
    try {
      if (FastParallelTestExecuter.DEBUG) testComm.log("Add new JVM: " + processor);

      synchronized (testTracking) {
        List<String> tests = new ArrayList<>();
        testTracking.put(processor, tests);
      }

    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  private void checkQueues(TestComm testComm) {
    testComm.log("Queued paused: " + paused.get());
    
    int lowest = lowestJVMQueueCount();
    if (lowest < testComm.getLowCost()) {
      if (paused.compareAndExchange(true, false)) {
        testComm.resumeTests("Low JVM Queue count");
      }
    } else if (lowest > (testComm.getLowCost() + 3)) {
      if (paused.compareAndExchange(false, true)) {
        testComm.pauseTests("JVM Queue is good and full");
      }

    }
  }


  
  public void addTest(TestClassRunInfo testClass) {
    synchronized (processors) {
      NamedTestClassProcessor processor;
      if (processors.size() < maxProcs) {
        TestClassProcessor rawProc = factory.create();
        rawProcessors.add(rawProc);
        Actor actor = actorFactory.createActor(rawProc);
        processor = new NamedTestClassProcessor("JVM Test Proc" + processors.size(),
            actor.getProxy(TestClassProcessor.class));
        actors.add(actor);
        processors.add(processor);

        addProcessor(processor);

        // if (FastParallelTestExecuter.DEBUG)
        testComm.log(getName() + " Start Processing tests with JVM:" + processor.getName() + " result proc:" + resultProcessor);
        processor.startProcessing(resultProcessor);
      } else {
//        System.out
//            .println("Use exisiting processor for " + testClass.getTestClassName() + " procs: " + processors.size()
//                + " max: " + maxProcs);
      }
    }

    addTestToProc(testClass);
    checkQueues(testComm);
  }

  private void addTestToProc(TestClassRunInfo testClass) {
    NamedTestClassProcessor processor = null;
    try {
      synchronized (testTracking) {
        Set<Entry<NamedTestClassProcessor,List<String>>> entrySet = testTracking.entrySet();
        int lowestCost = Integer.MAX_VALUE;

        
        StringBuilder sb = new StringBuilder();
        for (Entry<NamedTestClassProcessor,List<String>> entry : entrySet) {
          List<String> tests = entry.getValue();
          List<String> head = getQueueHead(tests);

          int cost = calcCost(entry.getValue());
          sb.append(getQueueOut(entry, head, cost));
        }
        
        List<Entry<NamedTestClassProcessor,List<String>>> entryList = new ArrayList<>();
        entryList.addAll(entrySet);
        Collections.shuffle(entryList);

        for (Entry<NamedTestClassProcessor,List<String>> entry : entryList) {
          int cost = calcCost(entry.getValue());
          if (cost < lowestCost) {
            processor = entry.getKey();
            lowestCost = cost;
          }
        }

        // if (FastParallelTestExecuter.DEBUG) {
        testComm.log("");
        testComm.log("LowCost=" + testComm.getLowCost() + " MaxOutstanding=" + testComm.getMaxOutstanding() + "\n" + sb);
        testComm.log("");
        // }

        if (processor == null) {
          processor = entryList.get(0).getKey();
        }

        testComm.log( getName() + " Add test class to JVM: " + processor.getName() + " test:" + testClass.getTestClassName() + testComm.getTestClasses().getTestDetails(testClass.getTestClassName()) + " #" + testsAdded.getAndIncrement() + "\n");

        List<String> tests = testTracking.get(processor);

       // testComm.log(getName() + " Tests for proc:" + tests);
        tests.add(testClass.getTestClassName());

        //testComm.log(getName() + " Send to proc:" + testClass.getTestClassName() + testComm.getTestClasses().getTestDetails(testClass.getTestClassName()) + " " + processor);
      }
      processor.processTestClass(testClass);

    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  private String getQueueOut(Entry<NamedTestClassProcessor,List<String>> entry, List<String> head, int cost) {
    return testComm.getName() + ":" + getName() + ":[" + entry.getKey().getName() + "] cost:" + cost + " -> " + head + (head.size() == entry.getValue().size() ? "" : "..." + entry.getValue().size()) + "\n";
  }

  private String getSimpleName(String test) {
    int index = test.lastIndexOf('.');
    String simpleName = test.substring(index + 1, test.length());
    return simpleName;
  }

  public void stop() {
    synchronized (processors) {
      testComm.log("\n" + getName() + " Stopping " + this.getClass().getSimpleName() + " after completing submitted tests ..."
          + processors + " " + actors + " " + resultProcessorActor);
    }

    while (!testComm.isDoneSend()) {
      synchronized (testComm.getDoneSendLock()) {
        try {
          testComm.getDoneSendLock().wait(15000);
        } catch (InterruptedException e) {
        }
      }
    }
    synchronized (processors) {
      try {
        CompositeStoppable.stoppable(processors).add(actors).add(resultProcessorActor).stop();
      } catch (DispatchException e) {
        throw UncheckedException.throwAsUncheckedException(e.getCause());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  private List<String> getQueueHead(List<String> tests) {
    List<String> head = new ArrayList<String>();

    synchronized (tests) {
      if (tests.size() > 3) {
        head.addAll(tests.subList(0, 3));
      } else {
        head.addAll(tests);
      }
    }

    for (int i = 0; i < head.size(); i++) {
      head.set(i, getSimpleName(head.get(i)) + getTestClasses().getTestDetails(head.get(i)));
    }

    return head;
  }

  private int calcCost(List<String> tests) {

    synchronized (tests) {
      int cost = 0;
      for (String test : tests) {
        if (getTestClasses().isSlowestTest(test)) {
          cost += 3;
        } else if (getTestClasses().isSlowTest(test)) {
          cost += 2;
        } else {
          cost += 1;
        }
      }
      return cost;
    }

  }

  public boolean lowJVMQueueCount() {
    StringBuilder sb = new StringBuilder();
    synchronized (testTracking) {
      
      Set<Entry<NamedTestClassProcessor,List<String>>> entrySet = testTracking.entrySet();

      for (Entry<NamedTestClassProcessor,List<String>> entry : entrySet) {

        List<String> tests = entry.getValue();
        List<String> head = getQueueHead(tests);

        int cost = calcCost(tests);
        sb.append(getQueueOut(entry, head, cost));
        if (cost < testComm.getLowCost()) {
          if (FastParallelTestExecuter.DEBUG) testComm.log("Found a JVM that needs tests.");
          return true;
        }
      }
    }

    //testComm.log("LowCost=" + testComm.getLowCost() + " MaxOutstanding=" + testComm.getMaxOutstanding() + "\n" + sb.toString());
    testComm.log(getName() + " queue sizes are okay ...\n");
    return false;
  }
  
  public int lowestJVMQueueCount() {
    int low = Integer.MAX_VALUE;
    StringBuilder sb = new StringBuilder();
    synchronized (testTracking) {

      Set<Entry<NamedTestClassProcessor,List<String>>> entrySet = testTracking.entrySet();

      for (Entry<NamedTestClassProcessor,List<String>> entry : entrySet) {

        List<String> tests = entry.getValue();
        List<String> head = getQueueHead(tests);

        int cost = calcCost(tests);
        sb.append(getQueueOut(entry, head, cost));
        if (cost < low) {
          low = cost;
        }
      }
    }

    //testComm.log(sb.toString());
    testComm.log("\n" + getName() + " low queue cost = " + low + "\n");
    return low;
  }
  
  public int totalJVMQueueCount() {
    int total = 0;
    StringBuilder sb = new StringBuilder();
    synchronized (testTracking) {
      Set<Entry<NamedTestClassProcessor,List<String>>> entrySet = testTracking.entrySet();

      for (Entry<NamedTestClassProcessor,List<String>> entry : entrySet) {
        List<String> tests = entry.getValue();
        int cost = calcCost(tests);
        List<String> head = getQueueHead(tests);
        sb.append(getQueueOut(entry, head, cost));
        total += cost;
      }
    }
    //testComm.log("LowCost=" + testComm.getLowCost() + " MaxOutstanding=" + testComm.getMaxOutstanding() + "\n" + sb.toString());
    testComm.log(getName() + " total queues cost = " + total);
    return total;
  }

  private String getName() {
    return "JVMs(" + maxProcs + ")";
  }

  public boolean emtpyQueues() {
    synchronized (testTracking) {
      Set<Entry<NamedTestClassProcessor,List<String>>> entrySet = testTracking.entrySet();

      for (Entry<NamedTestClassProcessor,List<String>> entry : entrySet) {

        if (entry.getValue().size() > 0) {
          return false;
        }
      }
    }

    return true;
  }

  public void startProcessing(TestResultProcessor resultProcessor) {
    // Create a processor that processes events in its own thread
    testComm.log(getName() + " Start processing");

    this.resultProcessorActor = actorFactory.createActor(resultProcessor);
    this.resultProcessor = resultProcessorActor.getProxy(TestResultProcessor.class);
  }

  public void stopNow() {
    for (TestClassProcessor processor : rawProcessors) {
      processor.stopNow();
    }
  }

}
