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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkStateWriter.ZkWriteCallback;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example leader selector client. Note that {@link LeaderSelectorListenerAdapter} which has the recommended handling
 * for connection state issues
 */
public class SolrSeer  implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final String name;
  private final LeaderSelector stateUpdateLeaderSelector;
  private final AtomicInteger leaderCount = new AtomicInteger();
  private final DistributedQueue<ZkNodeProps> stateUpdateQueue;
  public static final String QUEUE_OPERATION = "operation";
  private final DistributedQueue<ZkNodeProps> adminOperationQueue;
  private final LeaderSelector adminOperationLeaderSelector;
  private final ZkController zkController;
  private final OverseerCollectionMessageHandler collMessageHandler;
  private final ZkStateWriter zkStateWriter;
  
  private QueueSerializer<ZkNodeProps> serializer = new QueueSerializer<>() {

    @Override
    public byte[] serialize(ZkNodeProps item) {
      log.warn("SERIALIZE ITEM: " + item);
      return Utils.toJSON(item);
    }

    @Override
    public ZkNodeProps deserialize(byte[] bytes) {
   
      ZkNodeProps zkNodeProps = ZkNodeProps.load(bytes);
      
      log.warn("DESERIALIZE ITEM: " + zkNodeProps);
      return zkNodeProps;
    }

  };

  
  QueueConsumer<ZkNodeProps> stateUpdateConsumer = new QueueConsumer<>() {

    @Override
    public void consumeMessage(ZkNodeProps message) throws Exception {
      log.warn("Consume state update from queue {}", message);
      
    final String operation = message.getStr(QUEUE_OPERATION);
    if (operation == null) {
      throw new DW.Exp("Message missing " + QUEUE_OPERATION + ":" + message);
    }
    
    ClusterState clusterState = zkController.getZkStateReader().getClusterState();
    
     List<ZkWriteCommand> zkWriteOps = processMessage(clusterState, message, operation);
     
     zkStateWriter.enqueueUpdate(clusterState, zkWriteOps,  new ZkWriteCallback() {
      
      @Override
      public void onWrite() throws Exception {
        log.info("on write callback");
      }
      
    });
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      log.warn("state changed of state upate queue:" + newState);
    }
  };
  
  
  QueueConsumer<ZkNodeProps> adminOpConsumer = new QueueConsumer<>() {
    @Override
    public void consumeMessage(ZkNodeProps message) throws Exception {
      log.warn("Consume admin op form queue {}", message);
      final String operation = message.getStr(QUEUE_OPERATION);
      collMessageHandler.processMessage(message, operation);
      
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      log.warn("state changed of admin op queue:" + newState);
    }
  };
  
  
  public SolrSeer(ZkController zkController, SolrCloudManager cloudManager, ZkStateReader zkStateReader, CuratorFramework client, String path, String name) {
    this.zkController = zkController;
    this.name = name;
    
    collMessageHandler = new OverseerCollectionMessageHandler(
        zkController, cloudManager, zkController.getNodeName(),
        (HttpShardHandlerFactory) zkController.getCoreContainer().getShardHandlerFactory(),
        CommonParams.CORES_HANDLER_PATH, new Stats(), zkController.getCoreContainer(), this);

    zkStateWriter = new ZkStateWriter(zkStateReader, new Stats());
    this.stateUpdateQueue = QueueBuilder.builder(client, stateUpdateConsumer, serializer, "/solrseer/queues/stateupdate").lockPath("/solrseer/queues/stateupdate_lock").buildQueue();
    this.adminOperationQueue = QueueBuilder.builder(client, adminOpConsumer, serializer, "/solrseer/queues/adminop").lockPath("/solrseer/queues/adminop_lock").buildQueue();
    
    // create a leader selector using the given path for management
    // all participants in a given leader selection must use the same path
    // ExampleClient here is also a LeaderSelectorListener but this isn't required
    stateUpdateLeaderSelector = new LeaderSelector(client, "/solrseer/leaders/stateupdate_leader", new LeaderSelectorListenerAdapter() {

      @Override
      public void takeLeadership(CuratorFramework client) throws Exception {
        // we are now the leader. This method should not return until we want to relinquish leadership

        final int waitSeconds = (int) (5 * Math.random()) + 1;

        log.info(zkController.getNodeName() + "-StateUpdate is now the leader. Waiting " + waitSeconds + " seconds...");
        log.info(zkController.getNodeName() + "-StateUpdate has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
          log.info(zkController.getNodeName() + "-StateUpdate was interrupted.");
          Thread.currentThread().interrupt();
        } finally {
          log.info(zkController.getNodeName() + "-StateUpdate relinquishing leadership.\n");
        }
      }});

    // for most cases you will want your instance to requeue when it relinquishes leadership
    stateUpdateLeaderSelector.autoRequeue();
    
    adminOperationLeaderSelector = new LeaderSelector(client, "/solrseer/leaders/adminop_leader", new LeaderSelectorListenerAdapter() {

      @Override
      public void takeLeadership(CuratorFramework client) throws Exception {
        // we are now the leader. This method should not return until we want to relinquish leadership

        final int waitSeconds = (int) (5 * Math.random()) + 1;

        log.info(zkController.getNodeName() + "-AdminOp is now the leader. Waiting " + waitSeconds + " seconds...");
        log.info(zkController.getNodeName() + "-AdminOp has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
          log.info(zkController.getNodeName() + "-AdminOp was interrupted.");
          Thread.currentThread().interrupt();
        } finally {
          log.info(zkController.getNodeName() + "-AdminOp relinquishing leadership.\n");
        }
      }});

    // for most cases you will want your instance to requeue when it relinquishes leadership
    adminOperationLeaderSelector.autoRequeue();
  }

  public void start() throws Exception {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    stateUpdateLeaderSelector.start();
    this.stateUpdateQueue.start();
    
    adminOperationLeaderSelector.start();
    this.adminOperationQueue.start();
  }

  @Override
  public void close() throws IOException {

    try (DW worker = new DW(this)) {
      worker.add("SolrSeerInternals", stateUpdateQueue, stateUpdateLeaderSelector, adminOperationQueue, adminOperationLeaderSelector);
    }
    
//    stateUpdateQueue.close();
//    
//    stateUpdateLeaderSelector.interruptLeadership();
//    stateUpdateLeaderSelector.close();
//    
//    adminOperationQueue.close();
//    adminOperationLeaderSelector.close();
  }

  public void sendUpdate(ZkNodeProps msg) {
    try {
      stateUpdateQueue.put(msg);
      stateUpdateQueue.flushPuts(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new DW.Exp(e);
    }
  }
  
  public void sendAdminUpdate(ZkNodeProps msg) {
    try {
      adminOperationQueue.put(msg);
      adminOperationQueue.flushPuts(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new DW.Exp(e);
    }
  }
  
  private List<ZkWriteCommand> processMessage(ClusterState clusterState,
      final ZkNodeProps message, final String operation) {
      log.info("processMessage(ClusterState clusterState={}, ZkNodeProps message={}, String operation={}) - start",
          clusterState, message, operation);

    CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(operation);
    if (collectionAction != null) {
      switch (collectionAction) {
        case CREATE:
          return Collections
              .singletonList(new ClusterStateMutator(getSolrCloudManager()).createCollection(clusterState, message));
        case DELETE:
          return Collections
              .singletonList(new ClusterStateMutator(getSolrCloudManager()).deleteCollection(clusterState, message));
        case CREATESHARD:
          return Collections
              .singletonList(new CollectionMutator(getSolrCloudManager()).createShard(clusterState, message));
        case DELETESHARD:
          return Collections
              .singletonList(new CollectionMutator(getSolrCloudManager()).deleteShard(clusterState, message));
        case ADDREPLICA:
          return Collections.singletonList(new SliceMutator(getSolrCloudManager()).addReplica(clusterState, message));
        case ADDREPLICAPROP:
          return Collections
              .singletonList(new ReplicaMutator(getSolrCloudManager()).addReplicaProperty(clusterState, message));
        case DELETEREPLICAPROP:
          return Collections
              .singletonList(new ReplicaMutator(getSolrCloudManager()).deleteReplicaProperty(clusterState, message));
        case BALANCESHARDUNIQUE:
          ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
          if (dProp.balanceProperty()) {
            String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
            List<ZkWriteCommand> returnList = Collections
                .singletonList(new ZkWriteCommand(collName, dProp.getDocCollection()));
            if (log.isDebugEnabled()) {
              log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
            }
            return returnList;
          }
          break;
        case MODIFYCOLLECTION:
          CollectionsHandler.verifyRuleParams(zkController.getCoreContainer(), message.getProperties());
          return Collections
              .singletonList(new CollectionMutator(getSolrCloudManager()).modifyCollection(clusterState, message));
        case MIGRATESTATEFORMAT:
          return Collections
              .singletonList(new ClusterStateMutator(getSolrCloudManager()).migrateStateFormat(clusterState, message));
        default:
          throw new RuntimeException("unknown operation:" + operation
              + " contents:" + message.getProperties());
      }
    } else {
      OverseerAction overseerAction = OverseerAction.get(operation);
      if (overseerAction == null) {
        throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
      }
      switch (overseerAction) {
        case STATE:
          return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).setState(clusterState, message));
        case LEADER:
          return Collections
              .singletonList(new SliceMutator(getSolrCloudManager()).setShardLeader(clusterState, message));
        case DELETECORE:
          return Collections
              .singletonList(new SliceMutator(getSolrCloudManager()).removeReplica(clusterState, message));
        case ADDROUTINGRULE:
          return Collections
              .singletonList(new SliceMutator(getSolrCloudManager()).addRoutingRule(clusterState, message));
        case REMOVEROUTINGRULE:
          return Collections
              .singletonList(new SliceMutator(getSolrCloudManager()).removeRoutingRule(clusterState, message));
        case UPDATESHARDSTATE:
          return Collections
              .singletonList(new SliceMutator(getSolrCloudManager()).updateShardState(clusterState, message));
        case QUIT:
//          if (myId.equals(message.get(ID))) {
//            log.info("Quit command received {} {}", message, LeaderElector.getNodeName(myId));
//            overseerCollectionConfigSetProcessor.close();
//            close();
//          } else {
//            log.warn("Overseer received wrong QUIT message {}", message);
//          }
          break;
        case DOWNNODE:
          return new NodeMutator().downNode(clusterState, message);
        default:
          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
      }
    }

    List<ZkWriteCommand> returnList = Collections.singletonList(ZkStateWriter.NO_OP);
    if (log.isDebugEnabled()) {
      log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
    }
    return returnList;
  }

  private SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}