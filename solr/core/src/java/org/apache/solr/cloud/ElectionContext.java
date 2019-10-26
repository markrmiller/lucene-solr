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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.patterns.DW;
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
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ElectionContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final String electionPath;
  protected final ZkNodeProps leaderProps;
  protected final String id;
  protected final String leaderPath;
  protected volatile String leaderSeqPath;

  public ElectionContext(final String id, final String electionPath, final String leaderPath, final ZkNodeProps leaderProps) {
    this.id = id;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
  }
  
  public void close() {

  }
  
  public void cancelElection() throws InterruptedException, KeeperException {

  }

  abstract void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException, InterruptedException, IOException;

  public void checkIfIamLeaderFired() {}

  public void joinedElectionFired() {}

  public  ElectionContext copy(){
    throw new UnsupportedOperationException("copy");
  }
}

class ShardLeaderElectionContextBase extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrZkClient zkClient;

  private volatile Integer leaderZkNodeParentVersion;

  public ShardLeaderElectionContextBase(final String coreNodeName, String electionPath, String leaderPath,
      ZkNodeProps props, SolrZkClient zkClient) {
    super(coreNodeName, electionPath, leaderPath, props);
    this.zkClient = zkClient;
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    
    Integer version = leaderZkNodeParentVersion;
    if (version != null) {
      try {
        // We need to be careful and make sure we *only* delete our own leader registration node.
        // We do this by using a multi and ensuring the parent znode of the leader registration node
        // matches the version we expect - there is a setData call that increments the parent's znode
        // version whenever a leader registers.
        log.debug("Removing leader registration node on cancel: {} {}", leaderPath, version);
        List<Op> ops = new ArrayList<>(2);
        ops.add(Op.check(Paths.get(leaderPath).getParent().toString(), version));
        ops.add(Op.check(electionPath, -1));
        ops.add(Op.delete(leaderPath, -1));
        zkClient.multi(ops, true);
      } catch (Exception e) {
        throw new DW.Exp(e);
      } finally {
        version = null;
      }
    } else {
      log.info("No version found for ephemeral leader parent node, won't remove previous leader registration.");
    }

  }
  
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away

    String parent = Paths.get(leaderPath).getParent().toString();
    List<String> errors = new ArrayList<>();
    try {

      log.info("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);
      List<Op> ops = new ArrayList<>(3);

      // We use a multi operation to get the parent nodes version, which will
      // be used to make sure we only remove our own leader registration node.
      // The setData call used to get the parent version is also the trigger to
      // increment the version. We also do a sanity check that our leaderSeqPath exists.

      ops.add(Op.check(leaderSeqPath, -1));
      ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
      ops.add(Op.setData(parent, null, -1));
      List<OpResult> results;

      results = zkClient.multi(ops, true);
      Iterator<Op> it = ops.iterator();
      for (OpResult result : results) {
        if (result.getType() == ZooDefs.OpCode.setData) {
          SetDataResult dresult = (SetDataResult) result;
          Stat stat = dresult.getStat();
          leaderZkNodeParentVersion = stat.getVersion();
        }
        if (result.getType() == ZooDefs.OpCode.error) {
          ErrorResult dresult = (ErrorResult) result;
          if (dresult.getErr() > 0) {
            errors.add(it.next().getPath());
          }
        }
        
      }
      assert leaderZkNodeParentVersion != null;

    } catch (Throwable t) {
      throw new DW.Exp("Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed: " + errors, t);
    }
  }

  Integer getLeaderZkNodeParentVersion() {
    return leaderZkNodeParentVersion;
  }
}


// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final CoreContainer cc;
  private final SyncStrategy syncStrategy;
  
  protected final String shardId;
  protected final String collection;
  protected final LeaderElector leaderElector;

  private volatile boolean isClosed = false;

  private final ZkController zkController;
  
  public ShardLeaderElectionContext(LeaderElector leaderElector, 
      final String shardId, final String collection,
      final String coreNodeName, ZkNodeProps props, ZkController zkController, CoreContainer cc) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + "/leader_elect/" + shardId,  ZkStateReader.getShardLeadersPath(
            collection, shardId), props,
        zkController.getZkClient());
    this.cc = cc;
    this.syncStrategy = new SyncStrategy(cc);
    this.shardId = shardId;
    this.leaderElector = leaderElector;
    this.zkController = zkController;
    this.collection = collection;
  }
  
  @Override
  public void close() {
    super.close();
    this.isClosed  = true;
    syncStrategy.close();
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    try (SolrCore core = cc.getCore(coreName)) {
      if (core != null) {
        core.getCoreDescriptor().getCloudDescriptor().setLeader(false);
      }
    }
    
    super.cancelElection();
  }
  
  @Override
  public ElectionContext copy() {
    return new ShardLeaderElectionContext(leaderElector, shardId, collection, id, leaderProps, zkController, cc);
  }
  


  public LeaderElector getLeaderElector() {
    return leaderElector;
  }
  
  /* 
   * weAreReplacement: has someone else been the leader already?
   */
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStart) throws KeeperException,
      InterruptedException, IOException {
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    ActionThrottle lt;
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        // shutdown or removed
        return;
      }
      MDCLoggingContext.setCore(core);
      lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();
    }

    try {
      lt.minimumWaitBetweenActions();
      lt.markAttemptingAction();

      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

      log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId,
          weAreReplacement, leaderVoteWait);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
          ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection);
      zkController.getOverseer().getStateUpdateQueue().offer(Utils.toJSON(m));

      if (isClosed) {
        // Solr is shutting down or the ZooKeeper session expired while waiting for replicas. If the later,
        // we cannot be sure we are still the leader, so we should bail out. The OnReconnect handler will
        // re-register the cores and handle a new leadership election.
        return;
      }

      Replica.Type replicaType;
      String coreNodeName;
      boolean setTermToMax = false;
      try (SolrCore core = cc.getCore(coreName)) {

        if (core == null) {
          return;
        }
        CoreDescriptor cd = core.getCoreDescriptor();
        CloudDescriptor cloudCd = cd.getCloudDescriptor();
        replicaType = cloudCd.getReplicaType();
        coreNodeName = cloudCd.getCoreNodeName();
        // should I be leader?
        ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
        if (zkShardTerms.registered(coreNodeName) && !zkShardTerms.canBecomeLeader(coreNodeName)) {
          if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, cd, leaderVoteWait)) {
            rejoinLeaderElection(core);
            return;
          } else {
            // only log an error if this replica win the election
            setTermToMax = true;
          }
        }

        if (isClosed) {
          return;
        }

        log.info("I may be the new leader - try and sync");

        // nocommit
        // we are going to attempt to be the leader
        // first cancel any current recovery
        core.getUpdateHandler().getSolrCoreState().cancelRecovery();

        PeerSync.PeerSyncResult result = null;
        boolean success = false;
        try {
          result = syncStrategy.sync(zkController, core, leaderProps, weAreReplacement);
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
          rejoinLeaderElection(core);
          return;
        }

      }
      if (!isClosed) {
        try {
          if (replicaType == Replica.Type.TLOG) {
            // stop replicate from old leader
            zkController.stopReplicationFromLeader(coreName);
            if (weAreReplacement) {
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
          }
          // in case of leaderVoteWait timeout, a replica with lower term can win the election
          if (setTermToMax) {
            log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " +
                "without being up-to-date with the previous leader", coreNodeName);
            zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreNodeName);
          }
          super.runLeaderProcess(weAreReplacement, 0);

          assert shardId != null;

          ZkNodeProps zkNodes = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
              ZkStateReader.SHARD_ID_PROP, shardId,
              ZkStateReader.COLLECTION_PROP, collection,
              ZkStateReader.BASE_URL_PROP, leaderProps.get(ZkStateReader.BASE_URL_PROP),
              ZkStateReader.CORE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NAME_PROP),
              ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
          assert zkController != null;
          assert zkController.getOverseer() != null;
          zkController.getOverseer().offerStateUpdate(Utils.toJSON(zkNodes));

          try (SolrCore core = cc.getCore(coreName)) {
            if (core != null) {
              core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
              publishActiveIfRegisteredAndNotActive(core);
            } else {
              return;
            }
          }
          log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);

        } catch (Exception e) {
          SolrException.log(log, "There was a problem trying to register as the leader", e);

          if(e instanceof IOException
              || (e instanceof KeeperException && (!(e instanceof SessionExpiredException)))) {

            try (SolrCore core = cc.getCore(coreName)) {

              if (core == null) {
                if (log.isDebugEnabled())
                  log.debug("SolrCore not found:" + coreName + " in " + cc.getLoadedCoreNames());
                return;
              }
              core.getCoreDescriptor().getCloudDescriptor().setLeader(false);

              // we could not publish ourselves as leader - try and rejoin election
              try {
                rejoinLeaderElection(core);
              } catch (Exception exc) {
                throw new DW.Exp(e);
              }
            }
          } else {
            throw new DW.Exp(e);
          }
        }
      }

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
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    AtomicReference<Boolean> foundHigherTerm = new AtomicReference<>();
    try {
      zkController.getZkStateReader().waitForState(cd.getCollectionName(), timeout, TimeUnit.MILLISECONDS, (n,c) -> foundForHigherTermReplica(zkShardTerms, cd, foundHigherTerm));
    } catch (TimeoutException e) {
      log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (" +
          "core_term:{}, highest_term:{})",
      timeout, cd, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
      return true;
    }
    
    return false;
  }
  
  private boolean foundForHigherTermReplica(ZkShardTerms zkShardTerms, CoreDescriptor cd, AtomicReference<Boolean> foundHigherTerm) {
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
      log.info("Can't become leader, other replicas with higher term participated in leader election");
      foundHigherTerm.set(true);
      return true;
    }
    
    return false;
  }

  /**
   * Do other replicas with higher term participated in the election
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreNodeName) {
    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) return false;

    long replicaTerm = zkShardTerms.getTerm(coreNodeName);
    boolean isRecovering = zkShardTerms.isRecovering(coreNodeName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreNodeName)) continue;

      if (clusterState.getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());

        if (isRecovering && !isOtherReplicaRecovering) return true;
        if (otherTerm > replicaTerm) return true;
      }
    }
    return false;
  }

  public void publishActiveIfRegisteredAndNotActive(SolrCore core) throws Exception {
    if (log.isDebugEnabled()) log.debug("We have become the leader after core registration but are not in an ACTIVE state - publishing ACTIVE");
    zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
  }

  private void rejoinLeaderElection(SolrCore core)
      throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election
    if (cc.isShutDown()) {
      log.debug("Not rejoining election because CoreContainer is closed");
      return;
    }
    
    log.info("There may be a better leader candidate than us - going back into recovery");
    
    cancelElection();
    
    core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
    
    leaderElector.joinElection(this, true);
  }

}

final class OverseerElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;
  private final Overseer overseer;
  private volatile boolean isClosed = false;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, Overseer overseer) {
    super(zkNodeName, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", new ZkNodeProps(ID, zkNodeName), zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
    new ZkCmdExecutor(zkClient.getZkClientTimeout()).ensureExists(Overseer.OVERSEER_ELECT, zkClient);
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException,
      InterruptedException, IOException {
    if (isClosed) {
      return;
    }
    
    super.runLeaderProcess(weAreReplacement, pauseBeforeStartMs);

    synchronized (this) {
      if (!this.isClosed && !overseer.getZkController().getCoreContainer().isShutDown()) {
        overseer.start(id);
      }
    }
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    overseer.close();
  }
  
  @Override
  public void close() {
    this.isClosed  = true;
    overseer.close();
  }

  @Override
  public ElectionContext copy() {
    return new OverseerElectionContext(id, zkClient, overseer);
  }
  
  @Override
  public void joinedElectionFired() {

  }
  
  @Override
  public void checkIfIamLeaderFired() {

  }
}

