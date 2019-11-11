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
package org.apache.solr.cloud;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ID;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;

/**
 * A generic processor run in the Overseer, used for handling items added
 * to a distributed work queue.  Has support for handling exclusive tasks
 * (i.e. tasks that should not run in parallel with each other).
 *
 * An {@link OverseerMessageHandlerSelector} determines which
 * {@link OverseerMessageHandler} handles specific messages in the
 * queue.
 */
public class OverseerTaskProcessor implements Runnable, Closeable {

  /**
   * Maximum number of overseer collection operations which can be
   * executed concurrently
   */
  public static final int MAX_PARALLEL_TASKS = 100;
  public static final int MAX_BLOCKED_TASKS = 1000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerTaskQueue workQueue;
  private final DistributedMap runningMap;
  private final DistributedMap completedMap;
  private final DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  private final Set<String> runningTasks = ConcurrentHashMap.newKeySet(500);

  // List of completed tasks. This is used to clean up workQueue in zk.
  private final Map<String, QueueEvent> completedTasks = SW.concMapSmallO();

  private final String myId;

  private volatile boolean isClosed;

  private final Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks = ConcurrentHashMap.newKeySet(500);
  // This map may contain tasks which are read from work queue but could not
  // be executed because they are blocked or the execution queue is full
  // This is an optimization to ensure that we do not read the same tasks
  // again and again from ZK.
  final private Map<String, QueueEvent> blockedTasks = new ConcurrentSkipListMap<>();
  final private Predicate<String> excludedTasks = new Predicate<String>() {
    @Override
    public boolean test(String s) {
      if (log.isDebugEnabled()) {
        log.debug("$Predicate<String>.test(String s={}) - start", s);
      }

      boolean returnboolean = runningTasks.contains(s) || blockedTasks.containsKey(s);
      if (log.isDebugEnabled()) {
        log.debug("$Predicate<String>.test(String) - end");
      }
      return returnboolean;
    }

    @Override
    public String toString() {
      return StrUtils.join(ImmutableSet.of(runningTasks, blockedTasks.keySet()), ',');
    }

  };
  
  protected final OverseerMessageHandlerSelector selector;

  private final OverseerNodePrioritizer prioritizer;

  private final String thisNode;

  public OverseerTaskProcessor(ZkStateReader zkStateReader, String myId,
                                        Stats stats,
                                        OverseerMessageHandlerSelector selector,
                                        OverseerNodePrioritizer prioritizer,
                                        OverseerTaskQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.myId = myId;
    this.stats = stats;
    this.selector = selector;
    this.prioritizer = prioritizer;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    thisNode = Utils.getMDCNode();
  }

  @Override
  public void run() {
    if (log.isDebugEnabled()) {
      log.debug("run() - start");
    }

    MDCLoggingContext.setNode(thisNode);
    log.debug("Process current queue of overseer operations");

    String oldestItemInWorkQueue = null;
    // hasLeftOverItems - used for avoiding re-execution of async tasks that were processed by a previous Overseer.
    // This variable is set in case there's any task found on the workQueue when the OCP starts up and
    // the id for the queue tail is used as a marker to check for the task in completed/failed map in zk.
    // Beyond the marker, all tasks can safely be assumed to have never been executed.
    boolean hasLeftOverItems = true;

    try {
      oldestItemInWorkQueue = workQueue.getTailId();
    } catch (Exception e) {
      throw new SW.Exp(e);
    }

    if (oldestItemInWorkQueue == null)
      hasLeftOverItems = false;
    else
      log.debug("Found already existing elements in the work-queue. Last element: {}", oldestItemInWorkQueue);

    try {
      prioritizer.prioritizeOverseerNodes(myId);
    } catch (Exception e) {
      throw new SW.Exp(e);
    }

    try {
      while (!this.isClosed) {
        try {

          if (log.isDebugEnabled()) log.debug("Cleaning up work-queue. #Running tasks: {} #Completed tasks: {}",  runningTasksSize(), completedTasks.size());
          cleanUpWorkQueue();

          printTrackingMaps();

          ArrayList<QueueEvent> heads = new ArrayList<>(blockedTasks.size() + MAX_PARALLEL_TASKS);
          heads.addAll(blockedTasks.values());

          //If we have enough items in the blocked tasks already, it makes
          // no sense to read more items from the work queue. it makes sense
          // to clear out at least a few items in the queue before we read more items
          if (heads.size() < MAX_BLOCKED_TASKS) {
            //instead of reading MAX_PARALLEL_TASKS items always, we should only fetch as much as we can execute
            int toFetch = Math.min(MAX_BLOCKED_TASKS - heads.size(), MAX_PARALLEL_TASKS - runningTasksSize());
            List<QueueEvent> newTasks = workQueue.peekTopN(toFetch, excludedTasks, 2000L);
            log.debug("Got {} tasks from work-queue : [{}]", newTasks.size(), newTasks);
            heads.addAll(newTasks);
          }

//          if (heads.isEmpty()) {
//            log.debug()
//            continue;
//          }

          blockedTasks.clear(); // clear it now; may get refilled below.

          taskBatch.batchId++;
          boolean tooManyTasks = false;
          try (SW worker = new SW(this)) {

            for (QueueEvent head : heads) {
              if (!tooManyTasks) {
                tooManyTasks = runningTasksSize() >= MAX_PARALLEL_TASKS;
              }

              if (runningZKTasks.contains(head.getId())) {
                log.warn("Task found in running ZKTasks already, contining");
                continue;
              }

              final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
              final String asyncId = message.getStr(ASYNC);
              if (hasLeftOverItems) {
                if (head.getId().equals(oldestItemInWorkQueue))
                  hasLeftOverItems = false;
                if (asyncId != null && (completedMap.contains(asyncId) || failureMap.contains(asyncId))) {
                  log.debug("Found already processed task in workQueue, cleaning up. AsyncId [{}]", asyncId);
                  workQueue.remove(head);
                  continue;
                }
              }
              String operation = message.getStr(Overseer.QUEUE_OPERATION);
              if (operation == null) {
                log.error("Msg does not have required " + Overseer.QUEUE_OPERATION + ": {}", message);
                workQueue.remove(head);
                continue;
              }
              OverseerMessageHandler messageHandler = selector.selectOverseerMessageHandler(message);
              OverseerMessageHandler.Lock lock = messageHandler.lockTask(message, taskBatch);
              if (lock == null) {
                log.debug("Exclusivity check failed for [{}]", message.toString());
                // we may end crossing the size of the MAX_BLOCKED_TASKS. They are fine
                if (blockedTasks.size() < MAX_BLOCKED_TASKS)
                  blockedTasks.put(head.getId(), head);
                continue;
              }
              try {
                markTaskAsRunning(head, asyncId);
                log.debug("Marked task [{}] as running", head.getId());
              } catch (Exception e) {
                throw new SW.Exp(e);
              }
              log.debug(
                  messageHandler.getName() + ": Get the message id:" + head.getId() + " message:" + message.toString());
              Runner runner = new Runner(messageHandler, message,
                  operation, head, lock);
              worker.add(runner);
            }

          }

        } catch (Exception e) {
          throw new SW.Exp(e);
        }
      }
    } finally {
      this.close();
    }

    if (log.isDebugEnabled()) {
      log.debug("run() - end");
    }
  }

  private int runningTasksSize() {
    if (log.isDebugEnabled()) {
      log.debug("runningTasksSize() - start");
    }

    int returnint = runningTasks.size();
    if (log.isDebugEnabled()) {
      log.debug("runningTasksSize() - end");
    }
    return returnint;

  }

  private void cleanUpWorkQueue() throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("cleanUpWorkQueue() - start");
    }
    
    completedTasks.forEach((k,v) -> {try {
      workQueue.remove(v);
    } catch (KeeperException | InterruptedException e) {
      throw new SW.Exp(e);
    } runningTasks.remove(k);});

    completedTasks.clear();

    if (log.isDebugEnabled()) {
      log.debug("cleanUpWorkQueue() - end");
    }
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    isClosed = true;
    SW.close(selector);

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getSortedOverseerNodeNames(SolrZkClient zk={}) - start", zk);
    }

    List<String> children = null;
    try {
      children = zk.getChildren(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true);
    } catch (Exception e) {
      throw new SW.Exp("getSortedOverseerNodeNames(SolrZkClient=" + zk + ")", e);
    }
    LeaderElector.sortSeqs(children);
    ArrayList<String> nodeNames = new ArrayList<>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));

    if (log.isDebugEnabled()) {
      log.debug("getSortedOverseerNodeNames(SolrZkClient) - end");
    }
    return nodeNames;
  }

  public static List<String> getSortedElectionNodes(SolrZkClient zk, String path) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getSortedElectionNodes(SolrZkClient zk={}, String path={}) - start", zk, path);
    }

    List<String> children = null;
    try {
      children = zk.getChildren(path, null, true);
      LeaderElector.sortSeqs(children);

      if (log.isDebugEnabled()) {
        log.debug("getSortedElectionNodes(SolrZkClient, String) - end");
      }
      return children;
    } catch (Exception e) {
      throw new SW.Exp(e);
    }

  }

  public static String getLeaderNode(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getLeaderNode(SolrZkClient zkClient={}) - start", zkClient);
    }

    String id = getLeaderId(zkClient);
    String returnString = id == null ? null : LeaderElector.getNodeName(id);
    if (log.isDebugEnabled()) {
      log.debug("getLeaderNode(SolrZkClient) - end");
    }
    return returnString;
  }

  public static String getLeaderId(SolrZkClient zkClient) throws KeeperException,InterruptedException{
    if (log.isDebugEnabled()) {
      log.debug("getLeaderId(SolrZkClient zkClient={}) - start", zkClient);
    }

    byte[] data = null;
    try {
      data = zkClient.getData(Overseer.OVERSEER_ELECT + "/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      log.error("getLeaderId(SolrZkClient=" + zkClient + ")", e);

      if (log.isDebugEnabled()) {
        log.debug("getLeaderId(SolrZkClient) - end");
      }
      return null;
    }
    Map m = (Map) Utils.fromJSON(data);
    String returnString = (String) m.get(ID);
    if (log.isDebugEnabled()) {
      log.debug("getLeaderId(SolrZkClient) - end");
    }
    return  returnString;
  }

  public boolean isClosed() {
    return isClosed;
  }

  private void markTaskAsRunning(QueueEvent head, String asyncId)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("markTaskAsRunning(QueueEvent head={}, String asyncId={}) - start", head, asyncId);
    }

    runningZKTasks.add(head.getId());
    
    runningTasks.add(head.getId());
    
    if (asyncId != null)
      runningMap.put(asyncId, null);

    if (log.isDebugEnabled()) {
      log.debug("markTaskAsRunning(QueueEvent, String) - end");
    }
  }
  
  protected class Runner implements Runnable {
    final ZkNodeProps message;
    final String operation;
    volatile SolrResponse response;
    final QueueEvent head;
    final OverseerMessageHandler messageHandler;

    public Runner(OverseerMessageHandler messageHandler, ZkNodeProps message, String operation, QueueEvent head, OverseerMessageHandler.Lock lock) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
    }


    public void run() {
      if (log.isDebugEnabled()) {
        log.debug("run() - start");
      }

      String statsName = messageHandler.getTimerName(operation);
      final Timer.Context timerContext = stats.time(statsName);

      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String taskKey = messageHandler.getTaskKey(message);

      try {
        try {
          log.debug("Runner processing {}", head.getId());
          response = messageHandler.processMessage(message, operation);
        } finally {
          timerContext.stop();
          updateStats(statsName);
        }

        if (asyncId != null) {
          if (response != null && (response.getResponse().get("failure") != null 
              || response.getResponse().get("exception") != null)) {
            failureMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated failed map for task with zkid:[{}]", head.getId());
          } else {
            completedMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated completed map for task with zkid:[{}]", head.getId());
          }
        } else {
          head.setBytes(SolrResponse.serializable(response));
          log.debug("Completed task:[{}]", head.getId());
        }

        markTaskComplete(head.getId(), asyncId);
        log.debug("Marked task [{}] as completed.", head.getId());
        printTrackingMaps();

        log.debug(messageHandler.getName() + ": Message id:" + head.getId() +
            " complete, response:" + response.getResponse().toString());
        success = true;
      } catch (Exception e) {
        throw new SW.Exp(e);
      }

      if (log.isDebugEnabled()) {
        log.debug("run() - end");
      }
    }

    private void markTaskComplete(String id, String asyncId)
        throws KeeperException, InterruptedException {
      if (log.isDebugEnabled()) {
        log.debug("markTaskComplete(String id={}, String asyncId={}) - start", id, asyncId);
      }

      synchronized (completedTasks) {
        completedTasks.put(id, head);
      }

      synchronized (runningTasks) {
        runningTasks.remove(id);
      }

      if (asyncId != null) {
        if (!runningMap.remove(asyncId)) {
          log.warn("Could not find and remove async call [" + asyncId + "] from the running map.");
        }
      }

      workQueue.remove(head);

      if (log.isDebugEnabled()) {
        log.debug("markTaskComplete(String, String) - end");
      }
    }

    private void resetTaskWithException(OverseerMessageHandler messageHandler, String id, String asyncId, String taskKey, ZkNodeProps message) {
      log.warn("Resetting task: {}, requestid: {}, taskKey: {}", id, asyncId, taskKey);
      try {
        if (asyncId != null) {
          if (!runningMap.remove(asyncId)) {
            log.warn("Could not find and remove async call [" + asyncId + "] from the running map.");
          }
        }

        synchronized (runningTasks) {
          runningTasks.remove(id);
        }

      } catch (KeeperException e) {
        log.error("resetTaskWithException(OverseerMessageHandler=" + messageHandler + ", String=" + id + ", String=" + asyncId + ", String=" + taskKey + ", ZkNodeProps=" + message + ")", e);

        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        log.error("resetTaskWithException(OverseerMessageHandler=" + messageHandler + ", String=" + id + ", String=" + asyncId + ", String=" + taskKey + ", ZkNodeProps=" + message + ")", e);

        Thread.currentThread().interrupt();
      }

      if (log.isDebugEnabled()) {
        log.debug("resetTaskWithException(OverseerMessageHandler, String, String, String, ZkNodeProps) - end");
      }
    }

    private void updateStats(String statsName) {
      if (log.isDebugEnabled()) {
        log.debug("updateStats(String statsName={}) - start", statsName);
      }

      if (isSuccessful()) {
        stats.success(statsName);
      } else {
        stats.error(statsName);
        stats.storeFailureDetails(statsName, message, response);
      }

      if (log.isDebugEnabled()) {
        log.debug("updateStats(String) - end");
      }
    }

    private boolean isSuccessful() {
      if (log.isDebugEnabled()) {
        log.debug("isSuccessful() - start");
      }

      if (response == null)
        return false;
      boolean returnboolean = !(response.getResponse().get("failure") != null || response.getResponse().get("exception") != null);
      if (log.isDebugEnabled()) {
        log.debug("isSuccessful() - end");
      }
      return returnboolean;
    }
  }

  private void printTrackingMaps() {
    if (log.isDebugEnabled()) {
      synchronized (runningTasks) {
        log.debug("RunningTasks: {}", runningTasks.toString());
      }
      log.debug("BlockedTasks: {}", blockedTasks.keySet().toString());
      synchronized (completedTasks) {
        log.debug("CompletedTasks: {}", completedTasks.keySet().toString());
      }
      synchronized (runningZKTasks) {
        log.info("RunningZKTasks: {}", runningZKTasks.toString());
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("printTrackingMaps() - end");
    }
  }



  String getId(){
    return myId;
  }

  /**
   * An interface to determine which {@link OverseerMessageHandler}
   * handles a given message.  This could be a single OverseerMessageHandler
   * for the case where a single type of message is handled (e.g. collection
   * messages only) , or a different handler could be selected based on the
   * contents of the message.
   */
  public interface OverseerMessageHandlerSelector extends Closeable {
    OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message);
  }

  final private TaskBatch taskBatch = new TaskBatch();

  public class TaskBatch {
    private long batchId = 0;

    public long getId() {
      return batchId;
    }

    public int getRunningTasks() {
      if (log.isDebugEnabled()) {
        log.debug("getRunningTasks() - start");
      }
      int returnint = runningTasks.size();
      if (log.isDebugEnabled()) {
        log.debug("getRunningTasks() - end");
      }
      return returnint;
    }
  }

}