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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.PeerSyncWithLeader;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class may change in future and customisations are not supported between versions in terms of API or back compat
 * behaviour.
 *
 * @lucene.experimental
 */
public class RecoveryStrategy implements Runnable, Closeable {

  private final String collection;
  private final String shard;
  private volatile CountDownLatch latch;
  private volatile ReplicationHandler replicationHandler;
  private final Http2SolrClient recoveryOnlyClient;

  public static class Builder implements NamedListInitializedPlugin {
    private NamedList args;

    @Override
    public void init(NamedList args) {
      this.args = args;
    }

    // this should only be used from SolrCoreState
    public RecoveryStrategy create(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      final RecoveryStrategy recoveryStrategy = newRecoveryStrategy(cc, cd, recoveryListener);
      SolrPluginUtils.invokeSetters(recoveryStrategy, args);
      return recoveryStrategy;
    }

    protected RecoveryStrategy newRecoveryStrategy(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      return new RecoveryStrategy(cc, cd, recoveryListener);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile int waitForUpdatesWithStaleStatePauseMilliSeconds = Integer
      .getInteger("solr.cloud.wait-for-updates-with-stale-state-pause", 0);
  private volatile int maxRetries = Integer.getInteger("solr.recovery.maxretries", 500);
  private volatile int startingRecoveryDelayMilliSeconds = Integer
      .getInteger("solr.cloud.starting-recovery-delay-milli-seconds", 0);

  public static interface RecoveryListener {
    public void recovered();

    public void failed();
  }

  private volatile boolean close = false;
  private final RecoveryListener recoveryListener;
  private final ZkController zkController;
  private final String baseUrl;
  private final ZkStateReader zkStateReader;
  private final String coreName;
  private final AtomicInteger retries = new AtomicInteger(0);
  private boolean recoveringAfterStartup;
  private volatile Cancellable prevSendPreRecoveryHttpUriRequest;
  private volatile Replica.Type replicaType;

  private final CoreContainer cc;

  protected RecoveryStrategy(CoreContainer cc, CoreDescriptor cd, RecoveryListener recoveryListener) {
    // ObjectReleaseTracker.track(this);
    this.cc = cc;
    this.coreName = cd.getName();
    this.collection = cd.getCloudDescriptor().getCollectionName();
    this.shard  = cd.getCloudDescriptor().getShardId();

    this.recoveryListener = recoveryListener;
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();

    recoveryOnlyClient = new Http2SolrClient.Builder().
        withHttpClient(cc.getUpdateShardHandler().getRecoveryOnlyClient()).markInternalRequest().idleTimeout(60000).connectionTimeout(3000).build();
  }

  final public int getWaitForUpdatesWithStaleStatePauseMilliSeconds() {
    return waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public void setWaitForUpdatesWithStaleStatePauseMilliSeconds(
      int waitForUpdatesWithStaleStatePauseMilliSeconds) {
    this.waitForUpdatesWithStaleStatePauseMilliSeconds = waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public int getMaxRetries() {
    return maxRetries;
  }

  final public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  final public int getStartingRecoveryDelayMilliSeconds() {
    return startingRecoveryDelayMilliSeconds;
  }

  final public void setStartingRecoveryDelayMilliSeconds(int startingRecoveryDelayMilliSeconds) {
    this.startingRecoveryDelayMilliSeconds = startingRecoveryDelayMilliSeconds;
  }

  final public boolean getRecoveringAfterStartup() {
    return recoveringAfterStartup;
  }

  final public void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  // make sure any threads stop retrying
  @Override
  final public void close() {
    close = true;

    if (log.isDebugEnabled()) log.debug("Stopping recovery for core=[{}]", coreName);

    if (latch != null) {
      latch.countDown();
    }

    try {
      if (prevSendPreRecoveryHttpUriRequest != null) {
        prevSendPreRecoveryHttpUriRequest.cancel();
      }
      prevSendPreRecoveryHttpUriRequest = null;
    } catch (NullPointerException e) {
      // expected
    }

    ReplicationHandler finalReplicationHandler = replicationHandler;
    if (finalReplicationHandler != null) {

      finalReplicationHandler.abortFetch();
    }


    //ObjectReleaseTracker.release(this);
  }

  final private void recoveryFailed(final ZkController zkController, final String baseUrl, final CoreDescriptor cd) throws Exception {
    SolrException.log(log, "Recovery failed - I give up.");
    try {
      if (zkController.getZkClient().isAlive()) {
        zkController.publish(cd, Replica.State.RECOVERY_FAILED);
      }
    } finally {
      close();
      recoveryListener.failed();
    }
  }

  /**
   * This method may change in future and customisations are not supported between versions in terms of API or back
   * compat behaviour.
   *
   * @lucene.experimental
   */
  protected String getReplicateLeaderUrl(Replica leaderprops, ZkStateReader zkStateReader) {
    return leaderprops.getCoreUrl();
  }

  final private IndexFetcher.IndexFetchResult replicate(Replica leader)
      throws SolrServerException, IOException {

    log.info("Attempting to replicate from [{}].", leader);

    String leaderUrl;
    // send commit
    try {
      leaderUrl = leader.getCoreUrl();
      commitOnLeader(leaderUrl);
    } catch (Exception e) {
      if (e instanceof  SolrException && ((SolrException) e).getRootCause() instanceof RejectedExecutionException) {
       throw new AlreadyClosedException("An executor is shutdown already");
      }

      log.error("Commit on leader failed", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl);
    solrParams.set(ReplicationHandler.SKIP_COMMIT_ON_MASTER_VERSION_ZERO, replicaType == Replica.Type.TLOG);
    // always download the tlogs from the leader when running with cdcr enabled. We need to have all the tlogs
    // to ensure leader failover doesn't cause missing docs on the target

    boolean success = false;

    log.info("do replication fetch [{}].", solrParams);

    IndexFetcher.IndexFetchResult result = replicationHandler.doFetch(solrParams, retries.get() > 3);

    return result;

    // solrcloud_debug
//    if (log.isDebugEnabled()) {
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core
//            .getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        Directory dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.META_DATA, null);
//        try {
//          final IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
//          if (log.isDebugEnabled()) {
//            log.debug("{} replicated {} from {} gen: {} data: {} index: {} newIndex: {} files: {}"
//                , core.getCoreContainer().getZkController().getNodeName()
//                , searcher.count(new MatchAllDocsQuery())
//                , leaderUrl
//                , (null == commit ? "null" : commit.getGeneration())
//                , core.getDataDir()
//                , core.getIndexDir()
//                , core.getNewIndexDir()
//                , Arrays.asList(dir.listAll()));
//          }
//        } finally {
//          core.getDirectoryFactory().release(dir);
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        ParWork.propagateInterrupt(e);
//        log.debug("Error in solrcloud_debug block", e);
//      }
//    }

  }

  final private void commitOnLeader(String leaderUrl) throws SolrServerException,
      IOException {

    UpdateRequest ureq = new UpdateRequest();
    ureq.setBasePath(leaderUrl);
    ureq.setParams(new ModifiableSolrParams());
    ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, "terminal");
   // ureq.getParams().set("dist", false);
    // ureq.getParams().set(UpdateParams.OPEN_SEARCHER, onlyLeaderIndexes);// Why do we need to open searcher if
    // "onlyLeaderIndexes"?
    ureq.getParams().set(UpdateParams.OPEN_SEARCHER, false);

    log.info("send commit to leader {} {}", leaderUrl, ureq.getParams());
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false).process(recoveryOnlyClient);
    log.info("done send commit to leader {} {}", leaderUrl);
  }

  @Override
  final public void run() {
    // set request info for logging
    log.debug("Starting recovery process. recoveringAfterStartup={}", recoveringAfterStartup);
    try {
      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null) {
          log.warn("SolrCore is null, won't do recovery");
          throw new AlreadyClosedException("SolrCore is null, won't do recovery");
        }

        CoreDescriptor coreDescriptor = core.getCoreDescriptor();
        replicaType = coreDescriptor.getCloudDescriptor().getReplicaType();

        SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
        replicationHandler = (ReplicationHandler) handler;

        doRecovery(core, coreDescriptor);
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException, won't do recovery", e);
      return;
    } catch (AlreadyClosedException e) {
      log.info("AlreadyClosedException, won't do recovery", e);
      return;
    } catch (RejectedExecutionException e) {
      log.info("RejectedExecutionException, won't do recovery", e);
      return;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception during recovery", e);
      return;
    }
  }

  final public void doRecovery(SolrCore core, CoreDescriptor coreDescriptor) throws Exception {
    int tries = 0;
    while (!isClosed() && !core.isClosing() && !core.isClosed()) {
      tries++;
      try {
        try {
          if (prevSendPreRecoveryHttpUriRequest != null) {
            prevSendPreRecoveryHttpUriRequest.cancel();
          }
          prevSendPreRecoveryHttpUriRequest = null;
        } catch (NullPointerException e) {
          // expected
        }

        LeaderElector leaderElector = zkController.getLeaderElector(coreName);

        if (leaderElector != null && leaderElector.isLeader()) {
          log.info("We are the leader, STOP recovery");
          close = true;
          return;
        }

        Replica leader = zkController.getZkStateReader().getLeaderRetry(coreDescriptor.getCollectionName(), coreDescriptor.getCloudDescriptor().getShardId(), Integer.getInteger("solr.getleader.looptimeout", 8000));

        if (leader != null && leader.getName().equals(coreName)) {
          log.info("We are the leader in cluster state, REPEAT recovery");
          Thread.sleep(50);
          continue;
        }
        if (core.isClosing() || core.getCoreContainer().isShutDown()) {
          log.info("We are closing, STOP recovery");
          close = true;
          return;
        }
        boolean successfulRecovery;
        if (coreDescriptor.getCloudDescriptor().requiresTransactionLog()) {
          if (log.isDebugEnabled()) log.debug("Sync or replica recovery");
          successfulRecovery = doSyncOrReplicateRecovery(core, leader);
        } else {
          if (log.isDebugEnabled()) log.debug("Replicate only recovery");
          successfulRecovery = doReplicateOnlyRecovery(core, leader);
        }

        if (successfulRecovery) {
          close = true;
          break;
        } else {
          log.info("Trying another loop to recover after failing try={}", tries);
        }

      } catch (Exception e) {
        log.info("Exception trying to recover, try again try={}", tries, e);
      }
    }
  }

  final private boolean doReplicateOnlyRecovery(SolrCore core, Replica leader) throws Exception {
    boolean successfulRecovery = false;

    // if (core.getUpdateHandler().getUpdateLog() != null) {
    // SolrException.log(log, "'replicate-only' recovery strategy should only be used if no update logs are present, but
    // this core has one: "
    // + core.getUpdateHandler().getUpdateLog());
    // return;
    // }

    int cnt = 0;
    while (!successfulRecovery && !isClosed() && !core.isClosing() && !core.isClosed()) { // don't use interruption or
      // it will close channels
      // though
      cnt++;
      try {
        CoreDescriptor coreDescriptor = core.getCoreDescriptor();
        CloudDescriptor cloudDesc = coreDescriptor.getCloudDescriptor();

        try {

          LeaderElector leaderElector = zkController.getLeaderElector(coreName);

          if (leaderElector != null && leaderElector.isLeader()) {
            log.info("We are the leader, STOP recovery");
            close = true;
            return false;
          }

          leader = zkController.getZkStateReader().getLeaderRetry(coreDescriptor.getCollectionName(), coreDescriptor.getCloudDescriptor().getShardId(), Integer.getInteger("solr.getleader.looptimeout", 8000));

          if (leader != null && leader.getName().equals(coreName)) {
            log.info("We are the leader in cluster state, REPEAT recovery");
            Thread.sleep(50);
            continue;
          }

          if (leader != null && leader.getName().equals(coreName)) {
            log.info("We are the leader, STOP recovery");
            close = true;
            return false;
          }
        } catch (Exception e) {
          log.error("Could not get leader for {} {} {}", cloudDesc.getCollectionName(), cloudDesc.getShardId(), zkStateReader.getClusterState().getCollectionOrNull(cloudDesc.getCollectionName()), e);
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
        if (isClosed()) {
          throw new AlreadyClosedException();
        }
        log.info("Starting Replication Recovery. [{}] leader is [{}] and I am [{}] cnt={}", coreName, leader.getName(), Replica.getCoreUrl(baseUrl, coreName), cnt);

        try {
          log.info("Stopping background replicate from leader process");
          zkController.stopReplicationFromLeader(coreName);
          IndexFetcher.IndexFetchResult result = replicate(leader);

          if (result.getSuccessful()) {
            log.info("replication fetch reported as success");
          } else {
            log.error("replication fetch reported as failed: {} {} {}", result.getMessage(), result, result.getException());
            successfulRecovery = false;
            throw new SolrException(ErrorCode.SERVER_ERROR, "Replication fetch reported as failed");
          }

          log.info("Replication Recovery was successful.");
          successfulRecovery = true;
        } catch (Exception e) {
          log.error("Error while trying to recover", e);
          successfulRecovery = false;
        }

      } catch (Exception e) {
        log.error("Error while trying to recover. core=" + coreName, e);
        successfulRecovery = false;
      } finally {
        if (successfulRecovery) {
          log.info("Restarting background replicate from leader process");
          zkController.startReplicationFromLeader(coreName, false);
          log.info("Registering as Active after recovery.");
          try {
            zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
          } catch (Exception e) {
            log.error("Could not publish as ACTIVE after succesful recovery", e);
            successfulRecovery = false;
          }

          if (successfulRecovery) {
            recoveryListener.recovered();
          }
        }
      }

      if (!successfulRecovery) {
        // lets pause for a moment and we need to try again...
        // TODO: we don't want to retry for some problems?
        // Or do a fall off retry...
        try {

          log.error("Recovery failed - trying again... ({})", retries);

          if (retries.incrementAndGet() >= maxRetries) {
            close = true;
            log.error("Recovery failed - max retries exceeded (" + retries + ").");
            try {
              recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
            } catch (InterruptedException e) {

            } catch (Exception e) {
              log.error("Could not publish that recovery failed", e);
            }
          }
        } catch (Exception e) {
          log.error("An error has occurred during recovery", e);
        }
      }

      if (!successfulRecovery) {
        waitForRetry(core);
      } else {
        break;
      }
    }
    // We skip core.seedVersionBuckets(); We don't have a transaction log
    if (successfulRecovery) {
      close = true;
    }

    log.info("Finished recovery process, successful=[{}]", successfulRecovery);

    return successfulRecovery;
  }

  // TODO: perhaps make this grab a new core each time through the loop to handle core reloads?
  public final boolean doSyncOrReplicateRecovery(SolrCore core, Replica leader) throws Exception {
    log.debug("Do peersync or replication recovery core={} collection={}", coreName, core.getCoreDescriptor().getCollectionName());

    boolean successfulRecovery = false;
    boolean publishedActive = false;
    UpdateLog ulog;

    ulog = core.getUpdateHandler().getUpdateLog();
    if (ulog == null) {
      SolrException.log(log, "No UpdateLog found - cannot recover.");
      close = true;
      recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
      return false;
    }

    // we temporary ignore peersync for tlog replicas
    boolean firstTime = replicaType != Replica.Type.TLOG;

    boolean didReplication = false;

    List<Long> recentVersions;
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      recentVersions = recentUpdates.getVersions(ulog.getNumRecordsToKeep());
    } catch (Exception e) {
      log.error("Corrupt tlog - ignoring.", e);
      recentVersions = null;
    }

    List<Long> startingVersions = ulog.getStartingVersions();

    if (startingVersions != null && recentVersions != null && recoveringAfterStartup) {
      try {
        int oldIdx = 0; // index of the start of the old list in the current list
        long firstStartingVersion = startingVersions.size() > 0 ? startingVersions.get(0) : 0;

        for (; oldIdx < recentVersions.size(); oldIdx++) {
          if (recentVersions.get(oldIdx) == firstStartingVersion) break;
        }

        if (oldIdx > 0) {
          log.info("Found new versions added after startup: num=[{}]", oldIdx);
          if (log.isInfoEnabled()) {
            log.info("currentVersions size={} range=[{} to {}]", recentVersions.size(), recentVersions.get(0),
                recentVersions.get(recentVersions.size() - 1));
          }
        }

        if (startingVersions.isEmpty()) {
          log.debug("startupVersions is empty");
        } else {
          if (log.isDebugEnabled()) {
            log.debug("startupVersions size={} range=[{} to {}]", startingVersions.size(), startingVersions.get(0),
                startingVersions.get(startingVersions.size() - 1));
          }
        }
      } catch (Exception e) {
        log.error("Error getting recent versions.", e);
        recentVersions = Collections.emptyList();
      }
    }

    if (recoveringAfterStartup) {
      // if we're recovering after startup (i.e. we have been down), then we need to know what the last versions were
      // when we went down. We may have received updates since then.
      recentVersions = startingVersions;
      try {
        if (ulog.existOldBufferLog()) {
          // this means we were previously doing a full index replication
          // that probably didn't complete and buffering updates in the
          // meantime.
          log.info("Looks like a previous replication recovery did not complete - skipping peer sync.");
          firstTime = false; // skip peersync
        }
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        SolrException.log(log, "Error trying to get ulog starting operation.", e);
        firstTime = false; // skip peersync
      }
    }

    if (replicaType == Replica.Type.TLOG) {
      log.debug("Stopping replication from leader for {}", coreName);
      zkController.stopReplicationFromLeader(coreName);
    }

    log.debug("Publishing state of core [{}] as buffering {}", coreName, "doSyncOrReplicateRecovery");

    zkController.publish(core.getCoreDescriptor(), Replica.State.BUFFERING);

    Future<RecoveryInfo> replayFuture = null;
    int cnt = 0;
    while (!successfulRecovery && !isClosed() && !core.isClosing() && !core.isClosed()) {
      cnt++;
      try {

        log.debug("Begin buffering updates. core=[{}]", coreName);
        // recalling buffer updates will drop the old buffer tlog
        if (ulog.getState() != UpdateLog.State.BUFFERING) {
          ulog.bufferUpdates();
        }


        CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();

        LeaderElector leaderElector = zkController.getLeaderElector(coreName);

        if (leaderElector != null && leaderElector.isLeader()) {
          log.info("We are the leader, STOP recovery");
          close = true;
          return false;
        }

        DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);
        if (coll != null) {
          Slice slice = coll.getSlice(shard);
          if (slice != null) {
            Replica leaderReplica = slice.getLeader();
            if (leaderReplica != null) {
              if (leaderReplica.getNodeName().equals(cc.getZkController().getNodeName())) {
                leaderElector = cc.getZkController().getLeaderElector(leaderReplica.getName());
                if (leaderElector == null || !leaderElector.isLeader()) {
                  throw new SolrException(ErrorCode.BAD_REQUEST, leaderReplica.getName() + " is not current valid leader");
                }
              }
            }
          }
        }

        leader = zkController.getZkStateReader().getLeaderRetry(core.getCoreDescriptor().getCollectionName(), core.getCoreDescriptor().getCloudDescriptor().getShardId(), Integer.getInteger("solr.getleader.looptimeout", 8000));

        if (leader != null && leader.getName().equals(coreName)) {
          log.info("We are the leader in cluster state, REPEAT recovery");
          Thread.sleep(50);
          continue;
        }

        // we wait a bit so that any updates on the leader
        // that started before they saw recovering state
        // are sure to have finished (see SOLR-7141 for
        // discussion around current value)
        // TODO since SOLR-11216, we probably won't need this
//        try {
//          Thread.sleep(waitForUpdatesWithStaleStatePauseMilliSeconds);
//        } catch (InterruptedException e) {
//          ParWork.propagateInterrupt(e);
//          throw new SolrException(ErrorCode.SERVER_ERROR, e);
//        }

        // first thing we just try to sync
        if (firstTime) {
          firstTime = false; // only try sync the first time through the loop

          log.debug("Attempting to PeerSync from [{}] - recoveringAfterStartup=[{}]", leader.getCoreUrl(), recoveringAfterStartup);

          // System.out.println("Attempting to PeerSync from " + leaderUrl
          // + " i am:" + zkController.getNodeName());
          try {
            boolean syncSuccess;
            try (PeerSyncWithLeader peerSyncWithLeader = new PeerSyncWithLeader(core, leader.getCoreUrl(), ulog.getNumRecordsToKeep())) {
              syncSuccess = peerSyncWithLeader.sync(recentVersions).isSuccess();
            }
            if (syncSuccess) {
              SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
              log.debug("PeerSync was successful, commit to force open a new searcher");
              // force open a new searcher
              core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
              req.close();
              log.debug("PeerSync stage of recovery was successful.");

              // solrcloud_debug
              // cloudDebugLog(core, "synced");

              log.debug("Replaying updates buffered during PeerSync.");
              replay(core);

              // sync success
              successfulRecovery = true;
            } else {
              successfulRecovery = false;
            }

          } catch (Exception e) {
            log.error("PeerSync exception", e);
            successfulRecovery = false;
          }

          if (!successfulRecovery) {
            log.info("PeerSync Recovery was not successful - trying replication.");
          }
        }
        if (!successfulRecovery) {
          log.info("Starting Replication Recovery.");
          didReplication = true;
          try {

            // recalling buffer updates will drop the old buffer tlog
            if (ulog.getState() != UpdateLog.State.BUFFERING) {
              ulog.bufferUpdates();
            }

            try {
              if (prevSendPreRecoveryHttpUriRequest != null) {
                prevSendPreRecoveryHttpUriRequest.cancel();
              }
            } catch (NullPointerException e) {
              // okay
            }
            log.debug("Begin buffering updates. core=[{}]", coreName);


            sendPrepRecoveryCmd(leader.getBaseUrl(), leader.getName(), core.getCoreDescriptor());

            IndexFetcher.IndexFetchResult result = replicate(leader);

            if (result.getSuccessful()) {
              log.info("replication fetch reported as success");
            } else {
              log.error("replication fetch reported as failed: {} {} {}", result.getMessage(), result, result.getException());
              successfulRecovery = false;
              throw new SolrException(ErrorCode.SERVER_ERROR, "Replication fetch reported as failed");
            }

            replay(core);

            log.info("Replication Recovery was successful.");
            successfulRecovery = true;
          } catch (InterruptedException | AlreadyClosedException | RejectedExecutionException e) {
            log.info("{} bailing on recovery", e.getClass().getSimpleName());
            close = true;
            successfulRecovery = false;
            break;
          } catch (Exception e) {
            successfulRecovery = false;
            log.error("Error while trying to recover", e);
          }
        }
      } catch (Exception e) {
        log.error("Error while trying to recover. core=" + coreName, e);
        successfulRecovery = false;
      } finally {
        if (successfulRecovery) {
          log.info("Registering as Active after recovery {}", coreName);
          try {
            if (replicaType == Replica.Type.TLOG) {
              zkController.startReplicationFromLeader(coreName, true);
            }

            // if replay was skipped (possibly to due pulling a full index from the leader),
            // then we still need to update version bucket seeds after recovery
            if (successfulRecovery && replayFuture == null && didReplication) {
              log.info("Updating version bucket highest from index after successful recovery.");
              try {
                core.seedVersionBuckets();
              } catch (Exception e) {
                log.error("Exception seeding version buckets");
              }
            }

            zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
            publishedActive = true;
            close = true;

          } catch (AlreadyClosedException | RejectedExecutionException e) {
            log.error("Already closed");
            successfulRecovery = false;
            close = true;
          } catch (Exception e) {
            log.error("Could not publish as ACTIVE after successful recovery", e);
            successfulRecovery = false;
          }


        } else {
          log.info("Recovery was not successful, will not register as ACTIVE {}", coreName);
        }

        if (successfulRecovery) {
          recoveryListener.recovered();
        }
      }

      if (!successfulRecovery && !isClosed()) {
        // lets pause for a moment and we need to try again...
        // TODO: we don't want to retry for some problems?
        // Or do a fall off retry...
        try {
          log.error("Recovery failed - trying again... ({})", retries);

          if (retries.incrementAndGet() >= maxRetries) {
            SolrException.log(log, "Recovery failed - max retries exceeded (" + retries + ").");
            close = true;
            try {
              recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
            } catch (InterruptedException e) {

            } catch (Exception e) {
              log.error("Could not publish that recovery failed", e);
            }
          }
        } catch (Exception e) {
          log.error("An error has occurred during recovery", e);
        }
      }

      if (!successfulRecovery && !isClosed() && !core.isClosing() && !core.isClosed()) {
        waitForRetry(core);
      } else if (successfulRecovery) {
        break;
      }
    }

    log.debug("Finished doSyncOrReplicateRecovery process, successful=[{}]", successfulRecovery);

    if (successfulRecovery) {
      close = true;
    }

    if (successfulRecovery && !publishedActive) {
      log.error("Illegal state, successful recovery, but did not publish active");
      throw new SolrException(ErrorCode.SERVER_ERROR, "Illegal state, successful recovery, but did not publish active");
    }

    return successfulRecovery;
  }

  private final void waitForRetry(SolrCore core) {
    try {
      if (close) throw new AlreadyClosedException();
      long wait = startingRecoveryDelayMilliSeconds;

      if (retries.get() >= 0 && retries.get() < 10) {
        wait = 20;
      } else if (retries.get() >= 10 && retries.get() < 30) {
        wait = 1500;
      } else {
        wait = 10000;
      }

      log.info("Wait [{}] ms before trying to recover again (attempt={})", wait, retries);

      TimeOut timeout = new TimeOut(wait, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      while (!timeout.hasTimedOut()) {
        if (isClosed() && !core.isClosing() && !core.isClosed()) {
          log.info("RecoveryStrategy has been closed");
          return;
        }
        if (wait > 1000) {
          Thread.sleep(1000);
        } else {
          Thread.sleep(wait);
        }

      }

    } catch (InterruptedException e) {

    }

  }

  public static Runnable testing_beforeReplayBufferingUpdates;

  final private void replay(SolrCore core)
      throws InterruptedException, ExecutionException {
    if (testing_beforeReplayBufferingUpdates != null) {
      testing_beforeReplayBufferingUpdates.run();
    }

    if (replicaType == Replica.Type.TLOG) {
      // roll over all updates during buffering to new tlog, make RTG available
      try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
        core.getUpdateHandler().getUpdateLog().copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
      }
    }
    Future<RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().applyBufferedUpdates();
    if (future == null) {
      // no replay needed\
      log.debug("No replay needed.");
      return;
    } else {
      log.info("Replaying buffered documents.");
      // wait for replay
      RecoveryInfo report;
      try {
        report = future.get(10, TimeUnit.MINUTES); // MRM TODO: - how long? make configurable too
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      } catch (TimeoutException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      if (report.failed) {
        SolrException.log(log, "Replay failed");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      }
    }

    // the index may ahead of the tlog's caches after recovery, by calling this tlog's caches will be purged
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    if (ulog != null) {
      ulog.openRealtimeSearcher();
    }

    // solrcloud_debug
    // cloudDebugLog(core, "replayed");
  }

  final private void cloudDebugLog(SolrCore core, String op) {
    if (!log.isDebugEnabled()) {
      return;
    }
    try {
      RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
      SolrIndexSearcher searcher = searchHolder.get();
      try {
        final int totalHits = searcher.count(new MatchAllDocsQuery());
        final String nodeName = core.getCoreContainer().getZkController().getNodeName();
        log.debug("[{}] {} [{} total hits]", nodeName, op, totalHits);
      } finally {
        searchHolder.decref();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.debug("Error in solrcloud_debug block", e);
    }
  }

  final public boolean isClosed() {
    return close || cc.isShutDown();
  }

  final private void sendPrepRecoveryCmd(String leaderBaseUrl, String leaderCoreName, CoreDescriptor coreDescriptor) throws IOException, SolrServerException {

    if (coreDescriptor.getCollectionName() == null) {
      throw new IllegalStateException("Collection name cannot be null");
    }

    DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (coll != null) {
      Slice slice = coll.getSlice(shard);
      if (slice != null) {
        Replica leaderReplica = slice.getLeader();
        if (leaderReplica != null) {
          if (leaderReplica.getNodeName().equals(cc.getZkController().getNodeName())) {
            LeaderElector leaderElector = cc.getZkController().getLeaderElector(leaderReplica.getName());
            if (leaderElector == null || !leaderElector.isLeader()) {
              throw new SolrException(ErrorCode.BAD_REQUEST, leaderCoreName + " is not current valid leader");
            }
          }
        }
      }
    }

    WaitForState prepCmd = new WaitForState();
    prepCmd.setCoreName(coreName);
    prepCmd.setLeaderName(leaderCoreName);
    prepCmd.setState(Replica.State.BUFFERING);
    prepCmd.setCollection(coreDescriptor.getCollectionName());
    prepCmd.setShardId(coreDescriptor.getCloudDescriptor().getShardId());

    log.info("Sending prep recovery command to {} for leader={} params={}", leaderBaseUrl, leaderCoreName, prepCmd.getParams());

    int readTimeout = Integer.parseInt(System.getProperty("prepRecoveryReadTimeoutExtraWait", "30000"));

    if (isClosed()) {
      throw new AlreadyClosedException();
    }


      recoveryOnlyClient.setFakeAsync(true);
      prepCmd.setBasePath(leaderBaseUrl);

     // latch = new CountDownLatch(1);
      Cancellable result = recoveryOnlyClient.asyncRequest(prepCmd, null, new NamedListAsyncListener(latch, leaderCoreName));
      try {
        prevSendPreRecoveryHttpUriRequest = result;

      } finally {
        recoveryOnlyClient.waitForOutstandingRequests();
      }

  }

  private class NamedListAsyncListener implements AsyncListener<NamedList<Object>> {

    private final CountDownLatch latch;
    private final String leaderCoreName;

    public NamedListAsyncListener(CountDownLatch latch, String leaderCoreName) {
      this.latch = latch;
      this.leaderCoreName = leaderCoreName;
    }

    @Override
    public void onSuccess(NamedList<Object> entries) {

      prevSendPreRecoveryHttpUriRequest = null;
    }

    @Override
    public void onFailure(Throwable throwable, int code) {
      log.info("failed sending prep recovery cmd to leader response code={}", code, throwable);

      if (throwable != null && throwable.getMessage() != null && throwable.getMessage().contains("Not the valid leader")) {

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
        }

      }

    }
  }
}
