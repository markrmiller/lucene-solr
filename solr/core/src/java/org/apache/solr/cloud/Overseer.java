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

import static org.apache.solr.common.params.CommonParams.ID;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.patterns.DW.Exp;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.protobuf.Extension.MessageType;

/**
 * Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.
 */
public class Overseer implements SolrCloseable {
  public static final String OVERSEER_COLLECTION_QUEUE_WORK = "/overseer/collection-queue-work";

  public static final String OVERSEER_QUEUE = "/overseer/queue";

  public static final String OVERSEER_ASYNC_IDS = "/overseer/async_ids";

  public static final String OVERSEER_COLLECTION_MAP_FAILURE = "/overseer/collection-map-failure";

  public static final String OVERSEER_COLLECTION_MAP_COMPLETED = "/overseer/collection-map-completed";

  public static final String OVERSEER_COLLECTION_MAP_RUNNING = "/overseer/collection-map-running";

  public static final String OVERSEER_QUEUE_WORK = "/overseer/queue-work";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String QUEUE_OPERATION = "operation";

  // System properties are used in tests to make them run fast
  public static final int STATE_UPDATE_DELAY = ZkStateReader.STATE_UPDATE_DELAY;
  public static final int STATE_UPDATE_BATCH_SIZE = Integer.getInteger("solr.OverseerStateUpdateBatchSize", 10000);
  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer_elect";


  
  private class ClusterStateUpdater implements Closeable {
    private DistributedQueue<ZkNodeProps> stateUpdateQueue;
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    //private final ZkDistributedQueue stateUpdateQueue;
    //TODO remove in 9.0, we do not push message into this queue anymore
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue
   // private final ZkDistributedQueue workQueue;

    private volatile boolean isClosed = false;

    public ClusterStateUpdater(final ZkStateReader reader, final String myId, Stats zkStats) {
      this.zkClient = reader.getZkClient();
     // this.stateUpdateQueue = getStateUpdateQueue(zkStats);
    //  this.workQueue = getInternalWorkQueue(zkClient, zkStats);

      
      this.myId = myId;
      this.reader = reader;
    }

    @Override
    public void close() throws IOException {
    }

//    @Override
//    public void run() {
//      if (log.isDebugEnabled()) {
//        log.debug("Overseer run() - start");
//      }
//
//      MDCLoggingContext.setNode(zkController.getNodeName());
//
//      try {
//        if (log.isDebugEnabled()) {
//          log.debug("set watch on leader znode");
//        }
//        zkClient.exists(Overseer.OVERSEER_ELECT + "/leader", new Watcher() {
//
//          @Override
//          public void process(WatchedEvent event) {
//            if (EventType.None.equals(event.getType())) {
//              return;
//            }
//            log.info("Overseer leader has changed, closing ...");
//            Overseer.this.close();
//          }
//        }, true);
//      } catch (Exception e1) {
//        throw new DW.Exp(e1);
//      }
//
//      log.info("Starting to work on the main queue : {}", LeaderElector.getNodeName(myId));
//
//      ZkStateWriter zkStateWriter = null;
//      ClusterState clusterState = reader.getClusterState();
//
//      // we write updates in batch, but if an exception is thrown when writing new clusterstate,
//      // we do not sure which message is bad message, therefore we will re-process node one by one
//      int fallbackQueueSize = Integer.MAX_VALUE;
//   //   ZkDistributedQueue fallbackQueue = workQueue;
//      while (!this.isClosed) {
//        if (zkStateWriter == null) {
//          try {
//            zkStateWriter = new ZkStateWriter(reader, stats);
//            //
//            // // if there were any errors while processing
//            // // the state queue, items would have been left in the
//            // // work queue so let's process those first
//            // byte[] data = fallbackQueue.peek();
//            // while (fallbackQueueSize > 0 && data != null) {
//            // final ZkNodeProps message = ZkNodeProps.load(data);
//            // log.debug("processMessage: fallbackQueueSize: {}, message = {}",
//            // fallbackQueue.getZkStats().getQueueLength(), message);
//            // // force flush to ZK after each message because there is no fallback if workQueue items
//            // // are removed from workQueue but fail to be written to ZK
//            // try {
//            // clusterState = processQueueItem(message, clusterState, zkStateWriter, false, null);
//            // } catch (Exception e) {
//            // Exp exp = new DW.Exp(e);
//            // try {
//            // if (isBadMessage(e)) {
//            // log.warn(
//            // "Exception when process message = {}, consider as bad message and poll out from the queue",
//            // message);
//            // fallbackQueue.poll();
//            // }
//            // } catch (Exception e1) {
//            // DW.propegateInterrupt(e1);
//            // exp.addSuppressed(e1);
//            // }
//            //
//            // throw exp;
//            // }
//            // fallbackQueue.poll(); // poll-ing removes the element we got by peek-ing
//            // data = fallbackQueue.peek();
//            // fallbackQueueSize--;
//            // }
//            // // force flush at the end of the loop, if there are no pending updates, this is a no op call
//            // clusterState = zkStateWriter.writePendingUpdates();
//            // // the workQueue is empty now, use stateUpdateQueue as fallback queue
//            // fallbackQueue = stateUpdateQueue;
//            // fallbackQueueSize = 0;
//            // } catch (KeeperException.SessionExpiredException e) {
//            // log.error("run()", e);
//            //
//            // log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
//            // return;
//            // } catch (Exception e) {
//            // log.error("Exception in Overseer when process message from work queue, retrying", e);
//            //
//            // throw new DW.Exp(e);
//            // }
//            // }
//
//            LinkedList<Pair<String,byte[]>> queue = null;
//            try {
//              // We do not need to filter any nodes here cause all processed nodes are removed once we flush
//              // clusterstate
//              queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, 3000L, (x) -> true));
//            } catch (Exception e) {
//              throw new DW.Exp(e);
//            }
//            try {
//              Set<String> processedNodes = new HashSet<>();
//              while (queue != null && !queue.isEmpty()) {
//                for (Pair<String,byte[]> head : queue) {
//                  byte[] data = head.second();
//                  final ZkNodeProps message = ZkNodeProps.load(data);
//                  log.debug("processMessage: queueSize: {}, message = {} current state version: {}",
//                      stateUpdateQueue.getZkStats().getQueueLength(), message, clusterState.getZkClusterStateVersion());
//
//                  processedNodes.add(head.first());
//                  fallbackQueueSize = processedNodes.size();
//                  // The callback always be called on this thread
//                  clusterState = processQueueItem(message, clusterState, zkStateWriter, true, () -> {
//                    stateUpdateQueue.remove(processedNodes);
//                    processedNodes.clear();
//                  });
//                }
//                if (isClosed) break;
//                // if an event comes in the next 100ms batch it together
//                queue = new LinkedList<>(
//                    stateUpdateQueue.peekElements(1000, 100, node -> !processedNodes.contains(node)));
//              }
//              fallbackQueueSize = processedNodes.size();
//              // we should force write all pending updates because the next iteration might sleep until there
//              // are more items in the main queue
//              clusterState = zkStateWriter.writePendingUpdates();
//              // clean work queue
//              stateUpdateQueue.remove(processedNodes);
//              processedNodes.clear();
//            } catch (Exception e) {
//              throw new DW.Exp(e);
//            }
//
//          } finally {
//            log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));
//          }
//
//          if (log.isDebugEnabled()) {
//            log.debug("run() - end");
//          }
//        }
//      }
//    }

    // Return true whenever the exception thrown by ZkStateWriter is correspond
//    // to a invalid state or 'bad' message (in this case, we should remove that message from queue)
//    private boolean isBadMessage(Exception e) {
//      if (log.isDebugEnabled()) {
//        log.debug("isBadMessage(Exception e={}) - start", e);
//      }
//
//      if (e instanceof KeeperException) {
//        KeeperException ke = (KeeperException) e;
//        boolean isBadMessage = ke.code() == KeeperException.Code.NONODE || ke.code() == KeeperException.Code.NODEEXISTS;
//        if (log.isDebugEnabled()) {
//          log.debug("isBadMessage(Exception)={} - end", isBadMessage);
//        }
//        return isBadMessage;
//      }
//      if (log.isDebugEnabled()) {
//        log.debug("isBadMessage(Exception)=false - end");
//      }
//      return false;
//    }

//    private ClusterState processQueueItem(ZkNodeProps message, ClusterState clusterState, ZkStateWriter zkStateWriter, boolean enableBatching, ZkStateWriter.ZkWriteCallback callback) throws Exception {
//      if (log.isDebugEnabled()) {
//        log.debug("processQueueItem(ZkNodeProps message={}, ClusterState clusterState={}, ZkStateWriter zkStateWriter={}, boolean enableBatching={}, ZkStateWriter.ZkWriteCallback callback={}) - start", message, clusterState, zkStateWriter, enableBatching, callback);
//      }
//
//      final String operation = message.getStr(QUEUE_OPERATION);
//      if (operation == null) {
//        throw new DW.Exp("Message missing " + QUEUE_OPERATION + ":" + message);
//      }
//      List<ZkWriteCommand> zkWriteCommands = null;
//      final Timer.Context timerContext = stats.time(operation);
//      try {
//        zkWriteCommands = processMessage(clusterState, message, operation);
//        stats.success(operation);
//      } catch (Exception e) {
//        // generally there is nothing we can do - in most cases, we have
//        // an issue that will fail again on retry or we cannot communicate with     a
//        // ZooKeeper in which case another Overseer should take over
//        // TODO: if ordering for the message is not important, we could
//        // track retries and put it back on the end of the queue
//        log.error("Overseer could not process the current clusterstate state update message, skipping the message: " + message, e);
//        stats.error(operation);
//        
//        throw new DW.Exp(e);
//      } finally {
//        timerContext.stop();
//      }
//      if (zkWriteCommands != null) {
//        clusterState = zkStateWriter.enqueueUpdate(clusterState, zkWriteCommands, callback);
//        if (!enableBatching)  {
//          clusterState = zkStateWriter.writePendingUpdates();
//        }
//      }
//
//      if (log.isDebugEnabled()) {
//        log.debug("processQueueItem(ZkNodeProps, ClusterState, ZkStateWriter, boolean, ZkStateWriter.ZkWriteCallback) - end");
//      }
//      return clusterState;
//    }
//
//    private List<ZkWriteCommand> processMessage(ClusterState clusterState,
//        final ZkNodeProps message, final String operation) {
//      if (log.isDebugEnabled()) {
//        log.debug("processMessage(ClusterState clusterState={}, ZkNodeProps message={}, String operation={}) - start", clusterState, message, operation);
//      }
//
//      CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(operation);
//      if (collectionAction != null) {
//        switch (collectionAction) {
//          case CREATE:
//            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).createCollection(clusterState, message));
//          case DELETE:
//            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).deleteCollection(clusterState, message));
//          case CREATESHARD:
//            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).createShard(clusterState, message));
//          case DELETESHARD:
//            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).deleteShard(clusterState, message));
//          case ADDREPLICA:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).addReplica(clusterState, message));
//          case ADDREPLICAPROP:
//            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).addReplicaProperty(clusterState, message));
//          case DELETEREPLICAPROP:
//            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).deleteReplicaProperty(clusterState, message));
//          case BALANCESHARDUNIQUE:
//            ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
//            if (dProp.balanceProperty()) {
//              String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
//              List<ZkWriteCommand> returnList = Collections.singletonList(new ZkWriteCommand(collName, dProp.getDocCollection()));
//              if (log.isDebugEnabled()) {
//                log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
//              }
//              return returnList;
//            }
//            break;
//          case MODIFYCOLLECTION:
//            CollectionsHandler.verifyRuleParams(zkController.getCoreContainer() ,message.getProperties());
//            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).modifyCollection(clusterState,message));
//          case MIGRATESTATEFORMAT:
//            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).migrateStateFormat(clusterState, message));
//          default:
//            throw new RuntimeException("unknown operation:" + operation
//                + " contents:" + message.getProperties());
//        }
//      } else {
//        OverseerAction overseerAction = OverseerAction.get(operation);
//        if (overseerAction == null) {
//          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
//        }
//        switch (overseerAction) {
//          case STATE:
//            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).setState(clusterState, message));
//          case LEADER:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).setShardLeader(clusterState, message));
//          case DELETECORE:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).removeReplica(clusterState, message));
//          case ADDROUTINGRULE:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).addRoutingRule(clusterState, message));
//          case REMOVEROUTINGRULE:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).removeRoutingRule(clusterState, message));
//          case UPDATESHARDSTATE:
//            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).updateShardState(clusterState, message));
//          case QUIT:
//            if (myId.equals(message.get(ID))) {
//              log.info("Quit command received {} {}", message, LeaderElector.getNodeName(myId));
//              overseerCollectionConfigSetProcessor.close();
//              close();
//            } else {
//              log.warn("Overseer received wrong QUIT message {}", message);
//            }
//            break;
//          case DOWNNODE:
//            return new NodeMutator().downNode(clusterState, message);
//          default:
//            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
//        }
//      }
//
//      List<ZkWriteCommand> returnList = Collections.singletonList(ZkStateWriter.NO_OP);
//      if (log.isDebugEnabled()) {
//        log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
//      }
//      return returnList;
//    }

//    @Override
//    public void close() {
//      if (log.isDebugEnabled()) {
//        log.debug("close() - start");
//      }
//
//      this.isClosed = true;
//
//      if (log.isDebugEnabled()) {
//        log.debug("close() - end");
//      }
//    }

  }

  public static class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private Closeable thread;

    public OverseerThread(ThreadGroup tg, Closeable thread) {
      super(tg, (Runnable) thread);
      this.thread = thread;
    }

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void close() throws IOException {
      if (log.isDebugEnabled()) {
        log.debug("close() - start");
      }

      thread.close();
      this.isClosed = true;

      if (log.isDebugEnabled()) {
        log.debug("close() - end");
      }
    }

    public Closeable getThread() {
      return thread;
    }

    public boolean isClosed() {
      return this.isClosed;
    }

  }

  private volatile OverseerThread ccThread;

  private volatile OverseerThread updaterThread;

  private volatile OverseerThread triggerThread;

  private final ZkStateReader reader;

  private final HttpShardHandler shardHandler;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

 // private volatile OverseerCollectionConfigSetProcessor overseerCollectionConfigSetProcessor;

  private volatile ZkController zkController;

  private volatile Stats stats;
  private volatile String id;
  private volatile boolean closed;
  private volatile boolean systemCollCompatCheck = true;

  private final CloudConfig config;

  private volatile ClusterStateUpdater clusterStateUpdater;

  // overseer not responsible for closing reader
  public Overseer(HttpShardHandler shardHandler,
      UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, CloudConfig config)
      throws KeeperException, InterruptedException {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;
  }

  public synchronized void start(String id) {
    if (log.isDebugEnabled()) {
      log.debug("start(String id={}) - start", id);
    }

    MDCLoggingContext.setNode(zkController == null ?
        null :
        zkController.getNodeName());
    this.id = id;
    closed = false;
    doClose();
    stats = new Stats();
    log.info("Overseer (id=" + id + ") starting");
    //launch cluster state updater thread
 //   ThreadGroup tg = new ThreadGroup("Overseer state updater.");
   // updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id, stats), "OverseerStateUpdate-" + id);
   // updaterThread.setDaemon(true);

    clusterStateUpdater = new ClusterStateUpdater(reader, id, stats);
    
    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

//    OverseerNodePrioritizer overseerPrioritizer = new OverseerNodePrioritizer(reader, getStateUpdateQueue(), adminPath, shardHandler.getShardHandlerFactory(), updateShardHandler.getDefaultHttpClient());
//    overseerCollectionConfigSetProcessor = new OverseerCollectionConfigSetProcessor(reader, id, shardHandler, adminPath, stats, Overseer.this, overseerPrioritizer);
//    ccThread = new OverseerThread(ccTg, overseerCollectionConfigSetProcessor, "OverseerCollectionConfigSetProcessor-" + id);
//    ccThread.setDaemon(true);

    ThreadGroup triggerThreadGroup = new ThreadGroup("Overseer autoscaling triggers");
    OverseerTriggerThread trigger = new OverseerTriggerThread(zkController.getCoreContainer().getResourceLoader(),
        zkController.getSolrCloudManager(), config);
    triggerThread = new OverseerThread(triggerThreadGroup, trigger, "OverseerAutoScalingTriggerThread-" + id);

  //  updaterThread.start();
   // ccThread.start();
    triggerThread.start();

    systemCollectionCompatCheck(new BiConsumer<String, Object>() {
      boolean firstPair = true;
      @Override
      public void accept(String s, Object o) {
        if (log.isDebugEnabled()) {
          log.debug("$BiConsumer<String,Object>.accept(String s={}, Object o={}) - start", s, o);
        }

        if (firstPair) {
          log.warn("WARNING: Collection '.system' may need re-indexing due to compatibility issues listed below. See REINDEXCOLLECTION documentation for more details.");
          firstPair = false;
        }
        log.warn("WARNING: *\t{}:\t{}", s, o);
      }
    });

    assert ObjectReleaseTracker.track(this);

    if (log.isDebugEnabled()) {
      log.debug("start(String) - end");
    }
  }

  public void systemCollectionCompatCheck(final BiConsumer<String, Object> consumer) {
    if (log.isDebugEnabled()) {
      log.debug("systemCollectionCompatCheck(BiConsumer<String,Object> consumer={}) - start", consumer);
    }

    ClusterState clusterState = zkController.getClusterState();
    if (clusterState == null) {
      log.warn("Unable to check back-compat of .system collection - can't obtain ClusterState.");
      return;
    }
    DocCollection coll = clusterState.getCollectionOrNull(CollectionAdminParams.SYSTEM_COLL);
    if (coll == null) {
      if (log.isDebugEnabled()) {
        log.debug("systemCollectionCompatCheck(BiConsumer<String,Object>) - end");
      }
      return;
    }
    // check that all shard leaders are active
    boolean allActive = true;
    for (Slice s : coll.getActiveSlices()) {
      if (s.getLeader() == null || !s.getLeader().isActive(clusterState.getLiveNodes())) {
        allActive = false;
        break;
      }
    }
    if (allActive) {
      doCompatCheck(consumer);
    } else {
      // wait for all leaders to become active and then check
      zkController.zkStateReader.registerCollectionStateWatcher(CollectionAdminParams.SYSTEM_COLL, (liveNodes, state) -> {
        boolean active = true;
        if (state == null || liveNodes.isEmpty()) {
          return true;
        }
        for (Slice s : state.getActiveSlices()) {
          if (s.getLeader() == null || !s.getLeader().isActive(liveNodes)) {
            active = false;
            break;
          }
        }
        if (active) {
          doCompatCheck(consumer);
        }
        return active;
      });
    }

    if (log.isDebugEnabled()) {
      log.debug("systemCollectionCompatCheck(BiConsumer<String,Object>) - end");
    }
  }

  @SuppressWarnings("unchecked")
  private void doCompatCheck(BiConsumer<String, Object> consumer) {
    if (log.isDebugEnabled()) {
      log.debug("doCompatCheck(BiConsumer<String,Object> consumer={}) - start", consumer);
    }

    if (systemCollCompatCheck) {
      systemCollCompatCheck = false;
    } else {
      if (log.isDebugEnabled()) {
        log.debug("doCompatCheck(BiConsumer<String,Object>) - end");
      }
      return;
    }
    try (CloudSolrClient client = new CloudSolrClient.Builder(Collections.singletonList(getZkController().getZkServerAddress()), Optional.empty())
          .withSocketTimeout(30000).withConnectionTimeout(15000)
        .withHttpClient(updateShardHandler.getDefaultHttpClient()).build()) {
      CollectionAdminRequest.ColStatus req = CollectionAdminRequest.collectionStatus(CollectionAdminParams.SYSTEM_COLL)
          .setWithSegments(true)
          .setWithFieldInfo(true);
      CollectionAdminResponse rsp = req.process(client);
      NamedList<Object> status = (NamedList<Object>)rsp.getResponse().get(CollectionAdminParams.SYSTEM_COLL);
      Collection<String> nonCompliant = (Collection<String>)status.get("schemaNonCompliant");
      if (!nonCompliant.contains("(NONE)")) {
        consumer.accept("indexFieldsNotMatchingSchema", nonCompliant);
      }
      Set<Integer> segmentCreatedMajorVersions = new HashSet<>();
      Set<String> segmentVersions = new HashSet<>();
      int currentMajorVersion = Version.LATEST.major;
      String currentVersion = Version.LATEST.toString();
      segmentVersions.add(currentVersion);
      segmentCreatedMajorVersions.add(currentMajorVersion);
      NamedList<Object> shards = (NamedList<Object>)status.get("shards");
      for (Map.Entry<String, Object> entry : shards) {
        NamedList<Object> leader = (NamedList<Object>)((NamedList<Object>)entry.getValue()).get("leader");
        if (leader == null) {
          continue;
        }
        NamedList<Object> segInfos = (NamedList<Object>)leader.get("segInfos");
        if (segInfos == null) {
          continue;
        }
        NamedList<Object> infos = (NamedList<Object>)segInfos.get("info");
        if (((Number)infos.get("numSegments")).intValue() > 0) {
          segmentVersions.add(infos.get("minSegmentLuceneVersion").toString());
        }
        if (infos.get("commitLuceneVersion") != null) {
          segmentVersions.add(infos.get("commitLuceneVersion").toString());
        }
        NamedList<Object> segmentInfos = (NamedList<Object>)segInfos.get("segments");
        segmentInfos.forEach((k, v) -> {
          NamedList<Object> segment = (NamedList<Object>)v;
          segmentVersions.add(segment.get("version").toString());
          if (segment.get("minVersion") != null) {
            segmentVersions.add(segment.get("version").toString());
          }
          if (segment.get("createdVersionMajor") != null) {
            segmentCreatedMajorVersions.add(((Number)segment.get("createdVersionMajor")).intValue());
          }
        });
      }
      if (segmentVersions.size() > 1) {
        consumer.accept("differentSegmentVersions", segmentVersions);
        consumer.accept("currentLuceneVersion", currentVersion);
      }
      if (segmentCreatedMajorVersions.size() > 1) {
        consumer.accept("differentMajorSegmentVersions", segmentCreatedMajorVersions);
        consumer.accept("currentLuceneMajorVersion", currentMajorVersion);
      }

    } catch (SolrServerException | IOException e) {
      log.warn("Unable to perform back-compat check of .system collection", e);
    }

    if (log.isDebugEnabled()) {
      log.debug("doCompatCheck(BiConsumer<String,Object>) - end");
    }
  }

  public Stats getStats() {
    return stats;
  }

  ZkController getZkController(){
    return zkController;
  }

  public CoreContainer getCoreContainer() {
    if (log.isDebugEnabled()) {
      log.debug("getCoreContainer() - start");
    }

    CoreContainer returnCoreContainer = zkController.getCoreContainer();
    if (log.isDebugEnabled()) {
      log.debug("getCoreContainer() - end");
    }
    return returnCoreContainer;
  }

  public SolrCloudManager getSolrCloudManager() {
    if (log.isDebugEnabled()) {
      log.debug("getSolrCloudManager() - start");
    }

    SolrCloudManager returnSolrCloudManager = zkController.getSolrCloudManager();
    if (log.isDebugEnabled()) {
      log.debug("getSolrCloudManager() - end");
    }
    return returnSolrCloudManager;
  }

  /**
   * For tests.
   * 
   * @lucene.internal
   * @return state updater thread
   */
  public OverseerThread getUpdaterThread() {
    return updaterThread;
  }

  /**
   * For tests.
   * @lucene.internal
   * @return trigger thread
   */
  public OverseerThread getTriggerThread() {
    return triggerThread;
  }
  
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    if (this.id != null) {
      log.info("Overseer (id=" + id + ") closing");
    }
    this.closed = true;
    doClose();

    assert ObjectReleaseTracker.release(this);

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private void doClose() {
    if (log.isDebugEnabled()) {
      log.debug("doClose() - start");
    }

    try (DW closer = new DW(this)) {
      closer.collect(() -> {
        DW.close(ccThread);
        ccThread.interrupt();
      });

      closer.collect(() -> {
        DW.close(updaterThread);
        updaterThread.interrupt();
      });

      closer.collect(() -> {
        DW.close(triggerThread);
        triggerThread.interrupt();
      });

      closer.collect(() -> {
        try {
          updaterThread.join();
        } catch (InterruptedException e) {
          throw new DW.Exp(e);
        }
      });
      closer.collect(() -> {
        try {
          ccThread.join();
        } catch (InterruptedException e) {
          throw new DW.Exp(e);
        }
      });

      closer.collect(() -> {
        try {
          triggerThread.join();
        } catch (InterruptedException e) {
          throw new DW.Exp(e);
        }
      });
      
      closer.addCollect("OverseerInternals");
    }

    if (log.isDebugEnabled()) {
      log.debug("doClose() - end");
    }
  }

  /**
   * Get queue that can be used to send messages to Overseer.
   * <p>
   * Any and all modifications to the cluster state must be sent to
   * the overseer via this queue. The complete list of overseer actions
   * supported by this queue are documented inside the {@link OverseerAction} enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * Therefore, this method should be used only by clients for writing to the overseer queue.
   * <p>
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @return a {@link ZkDistributedQueue} object
   */
  DistributedQueue<ZkNodeProps> getStateUpdateQueue() {
    if (log.isDebugEnabled()) {
      log.debug("getStateUpdateQueue() - start");
    }

    if (log.isDebugEnabled()) {
      log.debug("getStateUpdateQueue() - end");
    }
    return clusterStateUpdater.stateUpdateQueue;
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients.
   * This method should not be used directly by anyone other than the Overseer itself.
   *
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  ZkDistributedQueue getStateUpdateQueue(Stats zkStats) {
    if (log.isDebugEnabled()) {
      log.debug("getStateUpdateQueue(Stats zkStats={}) - start", zkStats);
    }

    ZkDistributedQueue returnZkDistributedQueue = new ZkDistributedQueue(reader.getZkClient(), OVERSEER_QUEUE, zkStats, STATE_UPDATE_MAX_QUEUE, new ConnectionManager.IsClosed() {
      public boolean isClosed() {
        if (log.isDebugEnabled()) {
          log.debug("$ConnectionManager.IsClosed.isClosed() - start");
        }

        boolean returnboolean = Overseer.this.isClosed() || zkController.getCoreContainer().isShutDown();
        if (log.isDebugEnabled()) {
          log.debug("$ConnectionManager.IsClosed.isClosed() - end");
        }
        return returnboolean;
      }
    });
    if (log.isDebugEnabled()) {
      log.debug("getStateUpdateQueue(Stats) - end");
    }
    return returnZkDistributedQueue;
  }

  /**
   * Internal overseer work queue. This should not be used outside of Overseer.
   * <p>
   * This queue is used to store overseer operations that have been removed from the
   * state update queue but are being executed as part of a batch. Once
   * the result of the batch is persisted to zookeeper, these items are removed from the
   * work queue. If the overseer dies while processing a batch then a new overseer always
   * operates from the work queue first and only then starts processing operations from the
   * state update queue.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  static ZkDistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
    if (log.isDebugEnabled()) {
      log.debug("getInternalWorkQueue(SolrZkClient zkClient={}, Stats zkStats={}) - start", zkClient, zkStats);
    }

    ZkDistributedQueue returnZkDistributedQueue = new ZkDistributedQueue(zkClient, OVERSEER_QUEUE_WORK, zkStats);
    if (log.isDebugEnabled()) {
      log.debug("getInternalWorkQueue(SolrZkClient, Stats) - end");
    }
    return returnZkDistributedQueue;
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) {
    if (log.isDebugEnabled()) {
      log.debug("getRunningMap(SolrZkClient zkClient={}) - start", zkClient);
    }

    DistributedMap returnDistributedMap = new DistributedMap(zkClient, OVERSEER_COLLECTION_MAP_RUNNING);
    if (log.isDebugEnabled()) {
      log.debug("getRunningMap(SolrZkClient) - end");
    }
    return returnDistributedMap;
  }

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    if (log.isDebugEnabled()) {
      log.debug("getCompletedMap(SolrZkClient zkClient={}) - start", zkClient);
    }

    DistributedMap returnDistributedMap = new SizeLimitedDistributedMap(zkClient, OVERSEER_COLLECTION_MAP_COMPLETED, NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
    if (log.isDebugEnabled()) {
      log.debug("getCompletedMap(SolrZkClient) - end");
    }
    return returnDistributedMap;
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    if (log.isDebugEnabled()) {
      log.debug("getFailureMap(SolrZkClient zkClient={}) - start", zkClient);
    }

    DistributedMap returnDistributedMap = new SizeLimitedDistributedMap(zkClient, OVERSEER_COLLECTION_MAP_FAILURE, NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
    if (log.isDebugEnabled()) {
      log.debug("getFailureMap(SolrZkClient) - end");
    }
    return returnDistributedMap;
  }
  
  /* Map of async IDs currently in use*/
  static DistributedMap getAsyncIdsMap(final SolrZkClient zkClient) {
    if (log.isDebugEnabled()) {
      log.debug("getAsyncIdsMap(SolrZkClient zkClient={}) - start", zkClient);
    }

    DistributedMap returnDistributedMap = new DistributedMap(zkClient, OVERSEER_ASYNC_IDS);
    if (log.isDebugEnabled()) {
      log.debug("getAsyncIdsMap(SolrZkClient) - end");
    }
    return returnDistributedMap;
  }

  /**
   * Get queue that can be used to submit collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link CollectionsHandler} to submit collection API
   * tasks which are executed by the {@link OverseerCollectionMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient) {
    if (log.isDebugEnabled()) {
      log.debug("getCollectionQueue(SolrZkClient zkClient={}) - start", zkClient);
    }

    OverseerTaskQueue returnOverseerTaskQueue = getCollectionQueue(zkClient, new Stats());
    return returnOverseerTaskQueue;
  }

  /**
   * Get queue that can be used to read collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerCollectionMessageHandler} to read collection API
   * tasks submitted by the {@link CollectionsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats) {
    if (log.isDebugEnabled()) {
      log.debug("getCollectionQueue(SolrZkClient zkClient={}, Stats zkStats={}) - start", zkClient, zkStats);
    }

    OverseerTaskQueue returnOverseerTaskQueue = new OverseerTaskQueue(zkClient, OVERSEER_COLLECTION_QUEUE_WORK, zkStats);
    return returnOverseerTaskQueue;
  }

  /**
   * Get queue that can be used to submit configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link org.apache.solr.handler.admin.ConfigSetsHandler} to submit
   * tasks which are executed by the {@link OverseerConfigSetMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient)  {
    if (log.isDebugEnabled()) {
      log.debug("getConfigSetQueue(SolrZkClient zkClient={}) - start", zkClient);
    }

    OverseerTaskQueue returnOverseerTaskQueue = getConfigSetQueue(zkClient, new Stats());
    return returnOverseerTaskQueue;
  }

  /**
   * Get queue that can be used to read configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerConfigSetMessageHandler} to read configset API
   * tasks submitted by the {@link org.apache.solr.handler.admin.ConfigSetsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * <p>
   * For now, this internally returns the same queue as {@link #getCollectionQueue(SolrZkClient, Stats)}.
   * It is the responsibility of the client to ensure that configset API actions are prefixed with
   * {@link OverseerConfigSetMessageHandler#CONFIGSETS_ACTION_PREFIX} so that it is processed by
   * {@link OverseerConfigSetMessageHandler}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient, Stats zkStats) {
    if (log.isDebugEnabled()) {
      log.debug("getConfigSetQueue(SolrZkClient zkClient={}, Stats zkStats={}) - start", zkClient, zkStats);
    }

    // For now, we use the same queue as the collection queue, but ensure
    // that the actions are prefixed with a unique string.
    OverseerTaskQueue returnOverseerTaskQueue = getCollectionQueue(zkClient, zkStats);
    return returnOverseerTaskQueue;
  }
  
  public static boolean isLegacy(ZkStateReader stateReader) {
    if (log.isDebugEnabled()) {
      log.debug("isLegacy(ZkStateReader stateReader={}) - start", stateReader);
    }

    String legacyProperty = stateReader.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "false");
    boolean returnboolean = "true".equals(legacyProperty);
    return returnboolean;
  }

  public static boolean isLegacy(ClusterStateProvider clusterStateProvider) {
    if (log.isDebugEnabled()) {
      log.debug("isLegacy(ClusterStateProvider clusterStateProvider={}) - start", clusterStateProvider);
    }

    String legacyProperty = clusterStateProvider.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "false");
    boolean returnboolean = "true".equals(legacyProperty);
    return returnboolean;
  }

  public ZkStateReader getZkStateReader() {
    return reader;
  }

  public void offerStateUpdate(ZkNodeProps msg) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("offerStateUpdate(ZkNodeProps msg={}) - start", msg);
    }
    zkController.sendStateUpdate(msg);
   // getStateUpdateQueue().put(msg);

    if (log.isDebugEnabled()) {
      log.debug("offerStateUpdate(byte[]) - end");
    }
  }

  public boolean isOverSeerLeader() {
    return updaterThread.isAlive();
  }

}
