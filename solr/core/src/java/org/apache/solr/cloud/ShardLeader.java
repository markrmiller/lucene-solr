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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.patterns.SolrThreadSafe;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.name.Named;

@SolrThreadSafe
public class ShardLeader implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AtomicInteger leaderCount = new AtomicInteger();
  private final LeaderSelector shardLeaderSelector;
  private final CoreContainer cc;
  private final ZkController zkController;
  private final String collection;
  private final String shardId;
  private final ZkNodeProps leaderProps;
  private volatile boolean closed;
  
  private final Object closeObject = new Object();

  // @Inject
  ShardLeader(ZkController zkController, ZkNodeProps leaderProps, @Named("nodeName") String nodeName, String collection, String shardId) {
    this.cc = zkController.getCoreContainer();
    this.zkController = zkController;
    this.collection = collection;
    this.shardId = shardId;
    this.leaderProps = leaderProps;
    // create a leader selector using the given path for management
    // all participants in a given leader selection must use the same path
    // ExampleClient here is also a LeaderSelectorListener but this isn't required
    shardLeaderSelector = new LeaderSelector(zkController.getZkClient().getCurator(),
        "/collections/" + collection + "/" + shardId + "/shard_leader", new LeaderSelectorListenerAdapter() { // nocommit extract
      private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

          @Override
          public void takeLeadership(CuratorFramework client) throws Exception {
            if (log.isDebugEnabled()) {
              log.debug("$LeaderSelectorListenerAdapter.takeLeadership(CuratorFramework client={}) - start", client);
            }

            
            // we are now the leader. This method should not return until we want to relinquish leadership
            leaderCount.incrementAndGet();
            boolean success = runLeaderProcess();
            
            if (success) {
              log.info("We are the shard leader for shard={}, chillin!", shardId);
            } else {
              log.warn("We failed to become shard leader for shard={}", shardId);
            }
            
            if (success) {
              
              synchronized (closeObject) {
                while (!closed) {
                  closeObject.wait();
                }
              }
              log.info(zkController.getNodeName() + "-ShardLeader relinquishing leadership.\n");
            }
            
            // give up leadership and requeue

            if (log.isDebugEnabled()) {
              log.debug("$LeaderSelectorListenerAdapter.takeLeadership(CuratorFramework) - end");
            }
          }
        });
    
    shardLeaderSelector.autoRequeue();
  }
  
  public void start() {
    if (log.isDebugEnabled()) {
      log.debug("start() - start");
    }

    // shardLeaderSelector.setId(id);
    shardLeaderSelector.start();

    if (log.isDebugEnabled()) {
      log.debug("start() - end");
    }
  }

  
  boolean runLeaderProcess() throws KeeperException,
      InterruptedException, IOException {
    log.info("runLeaderProcess() - start");

    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    ActionThrottle lt;
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null || core.isClosed()) {
        // shutdown or removed
        log.info("runLeaderProcess() - end - no core found or core closed core={}", core);
        return false;
      }
      MDCLoggingContext.setCore(core);
      lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();
    }

    try {
      lt.minimumWaitBetweenActions();
      lt.markAttemptingAction();

      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

      log.info("Running the leader process for shard={} leaderVoteWait={}", shardId, leaderVoteWait);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
          ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.CORE_NODE_NAME_PROP, leaderProps.getStr(ZkStateReader.CORE_NODE_NAME_PROP));
      try {
        zkController.sendStateUpdate(m);
      } catch (Exception e1) {
        throw new DW.Exp(e1);
      }

      if (closed) {
        // Solr is shutting down or the ZooKeeper session expired while waiting for replicas. If the later,
        // we cannot be sure we are still the leader, so we should bail out. The OnReconnect handler will
        // re-register the cores and handle a new leadership election.

        log.info("runLeaderProcess() - end - ShardElector has been closed");
        return false;
      }

      Replica.Type replicaType;
      String coreNodeName;
      boolean setTermToMax = false;
      try (SyncStrategy syncStrategy = new SyncStrategy(cc)) {
        try (SolrCore core = cc.getCore(coreName)) {

          if (core == null || core.isClosed()) {
            log.info("runLeaderProcess() - end - no core found or core closed core={}", core);
            return false;
          }
          CoreDescriptor cd = core.getCoreDescriptor();
          CloudDescriptor cloudCd = cd.getCloudDescriptor();
          replicaType = cloudCd.getReplicaType();
          coreNodeName = cloudCd.getCoreNodeName();
          // should I be leader?
          ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
          if (zkShardTerms.registered(coreNodeName) && !zkShardTerms.canBecomeLeader(coreNodeName)) {
            if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, cd, leaderVoteWait)) {
              log.info("runLeaderProcess() - end - did not become eligable after timeout");
              return false;
            } else {
              // only log an error if this replica win the election
              setTermToMax = true;
            }
          }

          if (closed) {
            log.info("runLeaderProcess() - end - ShardElector has been closed");
            return false;
          }

          log.info("I may be the new leader - try and sync");

          // nocommit - review
          // we are going to attempt to be the leader
          // first cancel any current recovery
          core.getUpdateHandler().getSolrCoreState().cancelRecovery();

          PeerSync.PeerSyncResult result = null;
          boolean success = false;
          try {
            result = syncStrategy.sync(zkController, core, leaderProps);
            success = result.isSuccess();
          } catch (Exception e) {
            throw new DW.Exp(e);
          }

          UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

          if (!success) {
            boolean hasRecentUpdates = false;
            if (ulog != null) {
              // TODO: we could optimize this if necessary
              try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
                hasRecentUpdates = !recentUpdates.getVersions(1).isEmpty();
              }
            }

            if (!hasRecentUpdates) {
              // we failed sync, but we have no versions - we can't sync in that case
              // - we were active
              // before, so become leader anyway if no one else has any versions either
              if (result.getOtherHasVersions().orElse(false)) {
                log.info(
                    "We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader");
                success = false;
              } else {
                log.info(
                    "We failed sync, but we have no versions - we can't sync in that case - we were active before, so become leader anyway");
                success = true;
              }
            }
          }

          // solrcloud_debug
          if (log.isDebugEnabled()) {
            try {
              RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
              SolrIndexSearcher searcher = searchHolder.get();
              try {
                log.debug(core.getCoreContainer().getZkController().getNodeName() + " synched "
                    + searcher.count(new MatchAllDocsQuery()));
              } finally {
                searchHolder.decref();
              }
            } catch (Exception e) {
              throw new DW.Exp(e);
            }
          }
          if (!success) {
            log.info("runLeaderProcess() - end - sync attempt to become ShardLeader failed");
            return false;
          }

        }
      }

      try {
        if (replicaType == Replica.Type.TLOG) {
          // stop replicate from old leader
          zkController.stopReplicationFromLeader(coreName);

          try (SolrCore core = cc.getCore(coreName)) {
            Future<UpdateLog.RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().recoverFromCurrentLog();
            if (future != null) {
              log.info("Replaying tlog before become new leader");
              future.get();
            } else {
              log.info("New leader does not have old tlog to replay");
            }
          }

        }
        // in case of leaderVoteWait timeout, a replica with lower term can win the election
        if (setTermToMax) {
          log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " +
              "without being up-to-date with the previous leader", coreNodeName);
          zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreNodeName);
        }
        // super.runLeaderProcess(weAreReplacement, 0);
        
        // register as leader
        String leaderPath = ZkStateReader.getShardLeadersPath(collection, shardId);
      //  ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
     //   zkController.getZkClient().getCurator().createContainers(ZkStateReader.getShardLeadersParentPath(collection, shardId)); // nocommit - make efficient
        zkController.getZkClient().getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(leaderPath, Utils.toJSON(leaderProps)); // nocommit - acl's an stuff

        assert shardId != null;

        ZkNodeProps zkNodes = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
            ZkStateReader.SHARD_ID_PROP, shardId,
            ZkStateReader.COLLECTION_PROP, collection,
            ZkStateReader.BASE_URL_PROP, leaderProps.get(ZkStateReader.BASE_URL_PROP),
            ZkStateReader.CORE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NAME_PROP),
            ZkStateReader.CORE_NODE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NODE_NAME_PROP),
            ZkStateReader.NODE_NAME_PROP, leaderProps.get(ZkStateReader.NODE_NAME_PROP),
            ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
        assert zkController != null;

        zkController.sendStateUpdate(zkNodes);

        try (SolrCore core = cc.getCore(coreName)) {
          if (core != null) {
            core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
            zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
          } else {
            if (log.isDebugEnabled()) {
              log.debug("runLeaderProcess() - end");
            }
            return false;
          }
        }
        log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);
        return true;
      } catch (Throwable e) {
        DW.propegateInterrupt("There was a problem trying to register as the leader", e);

        if (e instanceof IOException
            || (e instanceof KeeperException && (!(e instanceof SessionExpiredException)))) {

          try (SolrCore core = cc.getCore(coreName)) {

            if (core == null) {
              log.info("runLeaderProcess() - end - no core found");
              return false;
            }
            core.getCoreDescriptor().getCloudDescriptor().setLeader(false);

            // we could not publish ourselves as leader - try and rejoin election
            // try {
            // rejoinLeaderElection(core);
            // } catch (Exception exc) {
            // throw new DW.Exp(e);
            // }
          }
        } else {
          throw new DW.Exp(e);
        }
      }

      log.info("runLeaderProcess() - end - failed becoming leader after exception");
      return false;
    } finally {
      MDCLoggingContext.clear();
    }
  }
 
  /**
   * Wait for other replicas with higher terms participate in the electioon
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, CoreDescriptor cd, int timeout) throws InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms={}, CoreDescriptor cd={}, int timeout={}) - start", zkShardTerms, cd, timeout);
    }

    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    AtomicReference<Boolean> foundHigherTerm = new AtomicReference<>();
    try {
      zkController.getZkStateReader().waitForState(cd.getCollectionName(), timeout, TimeUnit.MILLISECONDS, (n,c) -> foundForHigherTermReplica(zkShardTerms, cd, foundHigherTerm));
    } catch (TimeoutException e) {
      log.error("waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms=" + zkShardTerms + ", CoreDescriptor=" + cd + ", int=" + timeout + ")", e);

      log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (" +
          "core_term:{}, highest_term:{})",
      timeout, cd, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
      return true;
    }
    
    if (log.isDebugEnabled()) {
      log.debug("waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms, CoreDescriptor, int) - end");
    }
    return false;
  }
  
  private boolean foundForHigherTermReplica(ZkShardTerms zkShardTerms, CoreDescriptor cd, AtomicReference<Boolean> foundHigherTerm) {
    if (log.isDebugEnabled()) {
      log.debug("foundForHigherTermReplica(ZkShardTerms zkShardTerms={}, CoreDescriptor cd={}, AtomicReference<Boolean> foundHigherTerm={}) - start", zkShardTerms, cd, foundHigherTerm);
    }

    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
      log.info("Can't become leader, other replicas with higher term participated in leader election");
      foundHigherTerm.set(true);

      if (log.isDebugEnabled()) {
        log.debug("foundForHigherTermReplica(ZkShardTerms, CoreDescriptor, AtomicReference<Boolean>) - end");
      }
      return true;
    }
    
    if (log.isDebugEnabled()) {
      log.debug("foundForHigherTermReplica(ZkShardTerms, CoreDescriptor, AtomicReference<Boolean>) - end");
    }
    return false;
  }
  
  /**
   * Do other replicas with higher term participated in the election
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreNodeName) {
    if (log.isDebugEnabled()) {
      log.debug("replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms={}, String coreNodeName={}) - start", zkShardTerms, coreNodeName);
    }

    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) {
      return false;
    }

    long replicaTerm = zkShardTerms.getTerm(coreNodeName);
    boolean isRecovering = zkShardTerms.isRecovering(coreNodeName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreNodeName)) {
        continue;
      }

      if (clusterState.getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());

        if (isRecovering && !isOtherReplicaRecovering) {
          return true;
        }
        if (otherTerm > replicaTerm) {
          return true;
        }
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("replicasWithHigherTermParticipated(ZkShardTerms, String) - end");
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    this.closed = true;
    
    synchronized (closeObject) {
      closeObject.notifyAll();
    }
    
    try (DW worker = new DW(this, true)) {
    //  worker.add("ShardLeaderInterrupt", () -> {shardLeaderSelector.interruptLeadership();});
      worker.add("ShardLeaderInternals", shardLeaderSelector);
    }

    // stateUpdateQueue.close();
    //
    // stateUpdateLeaderSelector.interruptLeadership();
    // stateUpdateLeaderSelector.close();
    //
    // adminOperationQueue.close();
    // adminOperationLeaderSelector.close();

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }
}
