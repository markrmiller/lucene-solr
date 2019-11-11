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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.DefaultZkACLProvider;
import org.apache.solr.common.cloud.DefaultZkCredentialsProvider;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.patterns.SW.Exp;
import org.apache.solr.common.patterns.SolrThreadSafe;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrCoreInitializationException;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Handle ZooKeeper interactions.
 * <p>
 * notes: loads everything on init, creates what's not there - further updates are prompted with Watches.
 * <p>
 * TODO: exceptions during close on attempts to update cloud state
 */
@SolrThreadSafe
public class ZkController implements Closeable {
  static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final int WAIT_DOWN_STATES_TIMEOUT_SECONDS = 60;
  
  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  public static final byte[] emptyJson = "{}".getBytes(StandardCharsets.UTF_8);
  
 // private volatile ZkDistributedQueue overseerJobQueue;
  private volatile OverseerTaskQueue overseerCollectionQueue;
  private volatile OverseerTaskQueue overseerConfigSetQueue;
  private volatile DistributedMap overseerRunningMap;
  private volatile DistributedMap overseerCompletedMap;
  private volatile DistributedMap overseerFailureMap;
  private volatile DistributedMap asyncIdsMap;

  public static class ZkControllerData {
    public boolean SKIP_AUTO_RECOVERY;

    public ZkControllerData(boolean sKIP_AUTO_RECOVERY) {
      SKIP_AUTO_RECOVERY = sKIP_AUTO_RECOVERY;
    }
  }

  public final static String COLLECTION_PARAM_PREFIX = "collection.";
  public final static String CONFIGNAME_PROP = "configName";

  static class ContextKey {

    private String collection;
    private String coreNodeName;

    public ContextKey(String collection, String coreNodeName) {
      this.collection = collection;
      this.coreNodeName = coreNodeName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result
          + ((coreNodeName == null) ? 0 : coreNodeName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      ContextKey other = (ContextKey) obj;
      if (collection == null) {
        if (other.collection != null) return false;
      } else if (!collection.equals(other.collection)) return false;
      if (coreNodeName == null) {
        if (other.coreNodeName != null) return false;
      } else if (!coreNodeName.equals(other.coreNodeName)) return false;
      return true;
    }
  }
  
  private final SolrZkClient zkClient;
  private final ZkStateReader zkStateReader;
  private volatile SolrCloudManager cloudManager;
  private volatile CloudSolrClient cloudSolrClient;

  private final int localHostPort; // example: 54065
  private final String hostName; // example: 127.0.0.1
  private final String nodeName; // example: 127.0.0.1:54065_solr
  private volatile String baseURL; // example: http://127.0.0.1:54065/solr

  private final CloudConfig cloudConfig;
  private volatile NodesSysPropsCacher sysPropsCacher;

  private volatile LeaderElector overseerElector;

  private final Map<ContextKey,ElectionContext> electionContexts = SW.concMapSmallO();
  
  private final Set<ShardLeader> shardLeaders = SW.concSetSmallO();
  
  private final Map<String,ReplicateFromLeader> replicateFromLeaders = SW.concMapSmallO();
  private final Map<String,ZkCollectionTerms> collectionToTerms = SW.concMapSmallO();

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
 // final CoreContainer cc;

  protected volatile Overseer overseer;

  private final int leaderVoteWait;
  private final int leaderConflictResolveWait;

  private final boolean genericCoreNodeNames;

  private final int clientTimeout;

  private volatile boolean isClosed;

  private final ConcurrentHashMap<String,Throwable> replicasMetTragicEvent = SW.concMapSmallO();

  @Deprecated
  // keeps track of replicas that have been asked to recover by leaders running on this node
  private final Map<String,String> replicasInLeaderInitiatedRecovery = new HashMap<String,String>();

  // keeps track of a list of objects that need to know a new ZooKeeper session was created after expiration occurred
  // ref is held as a HashSet since we clone the set before notifying to avoid synchronizing too long
  final Set<OnReconnect> reconnectListeners = SW.concSetSmallO();
  final CurrentCoreDescriptorProvider registerOnReconnect;
  private volatile SolrSeer solrSeer;
  private final InterProcessReadWriteLock clusterWriteLock;
  private final CoreAccess coreAccess;
  private final CoreContainer cc;
 

  private class RegisterCoreAsync implements Callable<Object> {

    final CoreDescriptor descriptor;
    final boolean recoverReloadedCores;
    final boolean afterExpiration;

    RegisterCoreAsync(CoreDescriptor descriptor, boolean recoverReloadedCores, boolean afterExpiration) {
      this.descriptor = descriptor;
      this.recoverReloadedCores = recoverReloadedCores;
      this.afterExpiration = afterExpiration;
    }

    public Object call() throws Exception {
      log.info("Registering core {} afterExpiration? {}", descriptor.getName(), afterExpiration);
      register(descriptor.getName(), descriptor, recoverReloadedCores, afterExpiration, false);
      return descriptor;
    }
  }

  // notifies registered listeners after the ZK reconnect in the background
  static class OnReconnectNotifyAsync implements Callable<Object> {

    private final OnReconnect listener;

    OnReconnectNotifyAsync(OnReconnect listener) {
      this.listener = listener;
    }

    @Override
    public Object call() throws Exception {
      listener.command();
      return null;
    }
  }

  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig, final CurrentCoreDescriptorProvider registerOnReconnect, SolrZkClient zkClient)
      throws InterruptedException, TimeoutException, IOException {
    this.zkClient = zkClient;
    this.cc = cc;
    // nocommit - seeing how we can limit entanglement
    this.coreAccess = new CoreAccess() {
      @Override
      public List<CoreDescriptor> getCoreDescriptors() {
        return cc.getCoreDescriptors();
      }
      @Override
      public SolrCore getCore(String name) {
        return cc.getCore(name);
      }
    };

    this.cloudConfig = cloudConfig;
    this.registerOnReconnect = registerOnReconnect;
    this.genericCoreNodeNames = cloudConfig.getGenericCoreNodeNames();

    // be forgiving and strip this off leading/trailing slashes
    // this allows us to support users specifying hostContext="/" in
    // solr.xml to indicate the root context, instead of hostContext=""
    // which means the default of "solr"
    String localHostContext = trimLeadingAndTrailingSlashes(cloudConfig.getSolrHostContext());

    this.localHostPort = cloudConfig.getSolrHostPort();
    this.hostName = normalizeHostName(cloudConfig.getHost());
    this.nodeName = generateNodeName(this.hostName, Integer.toString(this.localHostPort), localHostContext);
    MDCLoggingContext.setNode(nodeName);
    this.leaderVoteWait = cloudConfig.getLeaderVoteWait();
    this.leaderConflictResolveWait = cloudConfig.getLeaderConflictResolveWait();

    this.clientTimeout = cloudConfig.getZkClientTimeout();
    DefaultConnectionStrategy strat = new DefaultConnectionStrategy();
    String zkACLProviderClass = cloudConfig.getZkACLProviderClass();
    ZkACLProvider zkACLProvider = null; // nocommit - give to curator
    if (zkACLProviderClass != null && zkACLProviderClass.trim().length() > 0) {
      zkACLProvider = cc.getResourceLoader().newInstance(zkACLProviderClass, ZkACLProvider.class);
    } else {
      zkACLProvider = new DefaultZkACLProvider();
    }

    String zkCredentialsProviderClass = cloudConfig.getZkCredentialsProviderClass();
    if (zkCredentialsProviderClass != null && zkCredentialsProviderClass.trim().length() > 0) {
      strat.setZkCredentialsToAddAutomatically(cc.getResourceLoader().newInstance(zkCredentialsProviderClass, ZkCredentialsProvider.class));
    } else {
      strat.setZkCredentialsToAddAutomatically(new DefaultZkCredentialsProvider());
    }
    addOnReconnectListener(getConfigDirListener());

    if (zkClient != null) {
      
    } else {
//      zkClient = new SolrZkClient(zkServerAddress, clientTimeout, zkClientConnectTimeout, strat,
//          new OnReconnectDoThis(this), new BeforeReconnect() {
//          @Override
//          public void command() {
//              closeOutstandingElections(registerOnReconnect);
//              markAllAsNotLeader(registerOnReconnect);
//            }
//          }, zkACLProvider, new ConnectionManager.IsClosed() {
//
//            @Override
//            public boolean isClosed() {
//              return ZkController.this.isClosed();
//            }
//          });

    }

    zkStateReader = new ZkStateReader(zkClient);
        
    clusterWriteLock = new InterProcessReadWriteLock(zkClient.getCurator(), "/CLUSTER_WRITE_LOCK");
    
    assert ObjectReleaseTracker.track(this);
  }
//
//  private boolean createZnodesForCluster(SolrZkClient zkClient) {
//    if (log.isDebugEnabled()) {
//      log.debug("createZnodesForCluster(SolrZkClient zkClient={}) - start", zkClient);
//    }
//
//    InterProcessMutex readLock = clusterWriteLock.readLock();
//    boolean writeClusterLayout = false;
//    try {
//      readLock.acquire();
//
//      if (zkClient.getCurator().checkExists().forPath(ZkStateReader.COLLECTIONS_ZKNODE) == null) {
//        writeClusterLayout = true;
//      }
//
//    } catch (Exception e1) {
//      log.error("createZnodesForCluster(SolrZkClient=" + zkClient + ")", e1);
//
//      throw new DW.Exp(e1);
//    } finally {
//      try {
//        readLock.release();
//      } catch (Exception e) {
//        log.error("createZnodesForCluster(SolrZkClient=" + zkClient + ")", e);
//
//        throw new DW.Exp(e);
//      }
//    }
//
//    if (log.isDebugEnabled()) {
//      log.debug("createZnodesForCluster(SolrZkClient) - end");
//    }
//    return writeClusterLayout;
//  }
//  

  
  public void start() {

    // nocommit todo state listener

    cloudSolrClient = new CloudSolrClient.Builder(new ZkClientClusterStateProvider(zkStateReader))
        .withHttpClient(cc.getUpdateShardHandler().getDefaultHttpClient()).build(); // nocommit
    cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), cloudSolrClient);
    
    solrSeer = new SolrSeer(this, cloudManager, zkStateReader, zkClient.getCurator(), Overseer.OVERSEER_QUEUE,
        "SolrSeer-" + getNodeName());
    

    // boolean writeClusterLayout = createZnodesForCluster(zkClient);

    boolean created = false;
    InterProcessMutex writeLock = clusterWriteLock.writeLock();
    try {
      log.info("try to aquire cluster write lock");
      writeLock.acquire();
      if (zkClient.getCurator().checkExists().forPath("/solrseer") == null) {
        try {
          log.info("Creating cluster znodes ...");
          createClusterZkNodes(zkClient);
          log.info("Creating cluster znodes done");
          created = true;
        } catch (Exception e) {
          throw new SW.Exp(e);
        }
      } else {
        log.info("Cluster layout found in ZooKeeper");
      }

    } catch (Exception e) {
      throw new SW.Exp(e);
    } finally {
      try {
        log.info("release cluster write lock");
        writeLock.release();
      } catch (Exception e) {
        throw new SW.Exp(e);
      }
    }

    CountDownLatch latch = new CountDownLatch(1);

    if (!created) {
      // nocommit TODO one live node watcher, but above zkstatereader
      Stat stat = null;
      try {
        log.info("Check for collections znode");
        stat = getZkClient().getCurator().checkExists().usingWatcher((CuratorWatcher) event -> {

          log.info("Got event on live node watcher {}", event.toString());
          if (event.getType() == EventType.NodeCreated) {
              latch.countDown();
          }

        }).forPath("/solrseer");
      } catch (Exception e) {
        throw new SW.Exp(e);
      }
      if (stat == null) {
        log.info("Collections znode not found, waiting on latch");
        try {
          latch.await();
          log.info("Done waiting on latch");
        } catch (InterruptedException e) {
          SW.propegateInterrupt(e);
        }
      }
    }
    log.info("Creat clusterstate and update");
    zkStateReader.createClusterStateWatchersAndUpdate();
    this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

    cloudManager.getClusterStateProvider().connect();


    try {
      solrSeer.start();
    } catch (Exception e) {
      throw new SW.Exp(e);
    }

    this.overseerRunningMap = Overseer.getRunningMap(zkClient);
    this.overseerCompletedMap = Overseer.getCompletedMap(zkClient);
    this.overseerFailureMap = Overseer.getFailureMap(zkClient);
    this.asyncIdsMap = Overseer.getAsyncIdsMap(zkClient);

    // overseerElector = new LeaderElector(zkClient);
    // try {
    // this.overseer = new Overseer((HttpShardHandler) cc.getShardHandlerFactory().getShardHandler(),
    // cc.getUpdateShardHandler(),
    // CommonParams.CORES_HANDLER_PATH, zkStateReader, this, cloudConfig);
    // ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient, overseer);
    // overseerElector.setup(context);
    // overseerElector.joinElection(context, false);
    //
    // } catch (Exception e) {
    // throw new DW.Exp(e);
    // }
    //
    // //this.overseerJobQueue = overseer.getStateUpdateQueue();
    // this.overseerCollectionQueue = overseer.getCollectionQueue(zkClient);
    // this.overseerConfigSetQueue = overseer.getConfigSetQueue(zkClient);

    this.sysPropsCacher = new NodesSysPropsCacher(getSolrCloudManager().getNodeStateProvider(),
        getNodeName(), zkStateReader);

    try {

      // start the overseer first as following code may need it's processing

      // Do this last to signal we're up.
      createEphemeralLiveNode();

      registerLiveNodesListener();

      publishAndWaitForDownStates(); // nocommit do on recconect

    } catch (Exception e) {
      throw new SW.Exp(e);
    }

  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  public NodesSysPropsCacher getSysPropsCacher() {
    return sysPropsCacher;
  }

  private void closeOutstandingElections(final CurrentCoreDescriptorProvider registerOnReconnect) {

    List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        closeExistingElectionContext(descriptor);
      }
    }
  }

  private ContextKey closeExistingElectionContext(CoreDescriptor cd) {
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);
    ElectionContext prevContext = electionContexts.get(contextKey);

    if (prevContext != null) {
      prevContext.close();
      electionContexts.remove(contextKey);
    }

    return contextKey;
  }

  private void markAllAsNotLeader(
      final CurrentCoreDescriptorProvider registerOnReconnect) {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        descriptor.getCloudDescriptor().setLeader(false);
        descriptor.getCloudDescriptor().setHasRegistered(false);
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    if (this.isClosed) { 
      throw new AlreadyClosedException();
    }
    this.isClosed = true;

    try (SW closer = new SW(this, true)) {
      closer.add("PublishNodeAsDown&RemoveEmphem", () -> {
        // if (getZkClient().getConnectionManager().isConnected()) { // nocommit
        try {
          log.info("Publish this node as DOWN...");
          publishNodeAsDown(getNodeName());
        } catch (Exception e) {
          SW.propegateInterrupt("Error publishing nodes as down. Continuing to close CoreContainer", e);
        }
        return "PublishDown";
        // }
      }, () -> {
        try {
          removeEphemeralLiveNode();
        } catch (Exception e) {
          SW.propegateInterrupt("Error publishing nodes as down. Continuing to close CoreContainer", e);
        }
        return "RemoveEphemNode";

      });
      // nocommit
      closer.add("Cleanup&Terms&RepFromLeaders", collectionToTerms, replicateFromLeaders);
      closer.add("ZkController Internals", overseerElector != null ? overseerElector.getContext() : null,
          electionContexts, shardLeaders, overseer, solrSeer,
          cloudManager, sysPropsCacher, cloudSolrClient, zkStateReader);
    } finally {
      assert ObjectReleaseTracker.release(this);
    }
  }

  /**
   * Best effort to give up the leadership of a shard in a core after hitting a tragic exception
   * @param cd The current core descriptor
   * @param tragicException The tragic exception from the {@code IndexWriter}
   */
  public void giveupLeadership(CoreDescriptor cd, Throwable tragicException) {
    assert tragicException != null;
    assert cd != null;
    DocCollection dc = getClusterState().getCollectionOrNull(cd.getCollectionName());
    if (dc == null) return;

    Slice shard = dc.getSlice(cd.getCloudDescriptor().getShardId());
    if (shard == null) return;

    // if this replica is not a leader, it will be put in recovery state by the leader
    if (shard.getReplica(cd.getCloudDescriptor().getCoreNodeName()) != shard.getLeader()) return;

    int numActiveReplicas = shard.getReplicas(
        rep -> rep.getState() == Replica.State.ACTIVE
            && rep.getType() != Type.PULL
            && getClusterState().getLiveNodes().contains(rep.getNodeName())
    ).size();

    // at least the leader still be able to search, we should give up leadership if other replicas can take over
    if (numActiveReplicas >= 2) {
      String key = cd.getCollectionName() + ":" + cd.getCloudDescriptor().getCoreNodeName();
      //TODO better handling the case when delete replica was failed
      if (replicasMetTragicEvent.putIfAbsent(key, tragicException) == null) {
        log.warn("Leader {} met tragic exception, give up its leadership", key, tragicException);
        try {
          // by using Overseer to remove and add replica back, we can do the task in an async/robust manner
          Map<String,Object> props = new HashMap<>();
          props.put(Overseer.QUEUE_OPERATION, "deletereplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(REPLICA_PROP, cd.getCloudDescriptor().getCoreNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));

          props.clear();
          props.put(Overseer.QUEUE_OPERATION, "addreplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().name().toUpperCase(Locale.ROOT));
          props.put(CoreAdminParams.NODE, getNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));
        } catch (Exception e) {
          throw new SW.Exp(e);
        }
      }
    }
  }


  /**
   * Returns true if config file exists
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null, true);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return zkStateReader.getClusterState();
  }

  public SolrCloudManager getSolrCloudManager() {
    return cloudManager;
  }

  /**
   * Returns config file data (in bytes)
   */
  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + zkConfigName + "/" + fileName;
    byte[] bytes = zkClient.getData(zkPath, null, null, true);
    if (bytes == null) {
      log.error("Config file contains no data:" + zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }

    return bytes;
  }

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  private String normalizeHostName(String host) throws IOException {

    if (host == null || host.length() == 0) {
      String hostaddress;
      try {
        hostaddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        hostaddress = "127.0.0.1"; // cannot resolve system hostname, fall through
      }
      // Re-get the IP again for "127.0.0.1", the other case we trust the hosts
      // file is right.
      if ("127.0.0.1".equals(hostaddress)) {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
          netInterfaces = NetworkInterface.getNetworkInterfaces();
          while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
              InetAddress ip = ips.nextElement();
              if (ip.isSiteLocalAddress()) {
                hostaddress = ip.getHostAddress();
              }
            }
          }
        } catch (Exception e) {
          throw new SW.Exp(e);
        }
      }
      host = hostaddress;
    } else {
      if (URLUtil.hasScheme(host)) {
        host = URLUtil.removeScheme(host);
      }
    }

    return host;
  }

  public String getHostName() {
    return hostName;
  }

  public int getHostPort() {
    return localHostPort;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * @return zookeeper server address
   */
  public String getZkServerAddress() {
    return zkClient.getZkServerAddress();
  }

  boolean isClosed() {
    return isClosed;
  }

  /**
   * Create the zknodes necessary for a cluster to operate
   *
   * @param zkClient a SolrZkClient
   * @throws KeeperException      if there is a Zookeeper error
   * @throws InterruptedException on interrupt
   */
  public static void createClusterZkNodes(SolrZkClient zkClient)
      throws KeeperException, InterruptedException, IOException {
    // nocommit
    AsyncCuratorFramework asyncClient = zkClient.getAsyncCurator();

    List<CuratorOp> operations = new ArrayList<>(30);
    
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.LIVE_NODES_ZKNODE));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.CONFIGS_ZKNODE));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.ALIASES, emptyJson));

   
    operations.add(asyncClient.transactionOp().create().forPath("/overseer"));
    operations.add(asyncClient.transactionOp().create().forPath("/overseer" + Overseer.OVERSEER_ELECT));
    operations.add(asyncClient.transactionOp().create().forPath("/overseer" + Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE));
    
    operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_QUEUE));
    operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_QUEUE_WORK));
   operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_COLLECTION_QUEUE_WORK));
   operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_COLLECTION_MAP_RUNNING));
    operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_COLLECTION_MAP_COMPLETED));
//    
    operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_COLLECTION_MAP_FAILURE));
    operations.add(asyncClient.transactionOp().create().forPath(Overseer.OVERSEER_ASYNC_IDS));

    operations.add(asyncClient.transactionOp().create().forPath("/autoscaling"));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH));
    operations.add(asyncClient.transactionOp().create().forPath("/autoscaling/events/.scheduled_maintenance"));
    operations.add(asyncClient.transactionOp().create().forPath("/autoscaling/events/.auto_add_replicas"));
//    
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.CLUSTER_STATE, emptyJson));
 //   operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.CLUSTER_PROPS, emptyJson));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_PKGS_PATH, emptyJson));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.ROLES, emptyJson));
//    
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_SECURITY_CONF_PATH, emptyJson));
    operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, emptyJson));
//    
//    // we create the collection znode last to indicate succesful cluster init
   // operations.add(asyncClient.transactionOp().create().forPath(ZkStateReader.COLLECTIONS_ZKNODE));
  
    operations.add(asyncClient.transactionOp().create().forPath("/solrseer"));
//    CreateBuilder create = zkClient.getCurator().create();
//    for (CuratorOp op : operations) {
//      try {
//        create.forPath(op.getTypeAndPath().getForPath());
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    }
//    
    
    try {
      zkClient.getCurator().create().forPath(ZkStateReader.CLUSTER_PROPS, emptyJson);
    } catch(NodeExistsException e  ) {
    
    }catch (Exception e2) {
      throw new SW.Exp(e2);
    }
  
    try {
      zkClient.getCurator().create().forPath(ZkStateReader.COLLECTIONS_ZKNODE);
    } catch (Exception e1) {
      throw new SW.Exp(e1);
    }
    log.info("Create new base SolrCloud znodes in ZooKeeper ({})", operations.size());
    
    

  // .create().forPath(ZkStateReader.LIVE_NODES_ZKNODE).create().forPath(ZkStateReader.LIVE_NODES_ZKNODE).create().forPath(ZkStateReader.LIVE_NODES_ZKNODE);
    
    
    
    CompletableFuture<Boolean> future = asyncClient.transaction().forOperations(operations).handle((l, t) -> {
    //  log.error("Error creating cluster znodes! results={} {} exception={} expresults={}", l, t, t != null ? ((KeeperException) t).getPath() : null);
         // t != null ? ((KeeperException) t).getPath() : "No Sub Results");
      
      if (l != null && l.size() > 0) {
        Iterator<CuratorTransactionResult> it = l.iterator();
        while (it.hasNext()) {
          CuratorTransactionResult result = it.next();
       //   log.error("ERRORRESULT: {}", result.getForPath());
        }
      }
      
      return t == null;
    }).toCompletableFuture();
    
    // zkClient.printLayout();
 //   log.error("suppress " + System.getProperty("solr.suppressDefaultConfigBootstrap"));
    if (!Boolean.getBoolean("solr.suppressDefaultConfigBootstrap")) {
      bootstrapDefaultConfigSet(zkClient);
    }
    
    
    try {
      boolean success = future.get();
      if (!success) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Multi Request failed");
      }
    } catch (ExecutionException e) {
      throw new SW.Exp(e);
    }
//    
 //   zkClient.printLayout();
    
//    boolean timeout = latch.await(15, TimeUnit.SECONDS);
//    if (timeout) throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout waiting for cluster state znodes to be created successfully");
  }

  private static void bootstrapDefaultConfigSet(SolrZkClient zkClient) throws KeeperException, InterruptedException, IOException {
    if (zkClient.exists("/configs/_default", true) == false) {
      String configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn("The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset" +
            " intended to be the default. Current 'solr.default.confdir' value: {}", System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        ZkMaintenanceUtils.upConfig(zkClient, Paths.get(configDirPath), ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
      }
    }
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from.
   * First tries the sysprop "solr.default.confdir". If not found, tries to find
   * the _default dir relative to the sysprop "solr.install.dir".
   * If that fails as well (usually for unit tests), tries to get the _default from the
   * classpath. Returns null if not found anywhere.
   */
  private static String getDefaultConfigDirPath() {
    String configDirPath = null;
    String serverSubPath = "solr" + File.separator +
        "configsets" + File.separator + "_default" +
        File.separator + "conf";
    String subPath = File.separator + "server" + File.separator + serverSubPath;
    if (System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE) != null && new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE)).exists()) {
      configDirPath = new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE)).getAbsolutePath();
    } else if (System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) != null &&
        new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath).exists()) {
      configDirPath = new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath).getAbsolutePath();
    } else { // find "_default" in the classpath. This one is used for tests
      configDirPath = getDefaultConfigDirFromClasspath(serverSubPath);
    }
    return configDirPath;
  }

  private static String getDefaultConfigDirFromClasspath(String serverSubPath) {
    URL classpathUrl = ZkController.class.getClassLoader().getResource(serverSubPath);
    try {
      if (classpathUrl != null && new File(classpathUrl.toURI()).exists()) {
        return new File(classpathUrl.toURI()).getAbsolutePath();
      }
    } catch (URISyntaxException ex) {}
    return null;
  }
  
  private void registerLiveNodesListener() {
    // this listener is used for generating nodeLost events, so we check only if
    // some nodes went missing compared to last state
    LiveNodesListener listener = (oldNodes, newNodes) -> {
      oldNodes.removeAll(newNodes);
      if (oldNodes.isEmpty()) { // only added nodes
        return false;
      }
      if (isClosed) {
        return true;
      }
      // if this node is in the top three then attempt to create nodeLost message
      int i = 0;
      for (String n : newNodes) {
        if (n.equals(getNodeName())) {
          break;
        }
        if (i > 2) {
          return false; // this node is not in the top three
        }
        i++;
      }

      // retrieve current trigger config - if there are no nodeLost triggers
      // then don't create markers
      boolean createNodes = false;
      try {
        createNodes = zkStateReader.getAutoScalingConfig().hasTriggerForEvents(TriggerEventType.NODELOST);
      } catch (KeeperException | InterruptedException e1) {
        SW.propegateInterrupt("Unable to read autoscaling.json", e1);
      }
      if (createNodes) {
        byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", cloudManager.getTimeSource().getEpochTimeNs()));
        for (String n : oldNodes) {
          String path = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + n;

          try {
            zkClient.create(path, json, CreateMode.PERSISTENT, true);
          } catch (KeeperException.NodeExistsException e) {
            // someone else already created this node - ignore
          } catch (KeeperException | InterruptedException e1) {
            SW.propegateInterrupt("Unable to register nodeLost path for " + n, e1);
          }
        }
      }
      return false;
    };
    zkStateReader.registerLiveNodesListener(listener);
  }

  public void publishAndWaitForDownStates() throws KeeperException,
  InterruptedException {
    publishAndWaitForDownStates(WAIT_DOWN_STATES_TIMEOUT_SECONDS);
  }

  public void publishAndWaitForDownStates(int timeoutSeconds) throws KeeperException,
      InterruptedException {

    publishNodeAsDown(getNodeName());
    List<CoreDescriptor> descriptors = coreAccess.getCoreDescriptors();
    Set<String> collectionsWithLocalReplica = ConcurrentHashMap.newKeySet(descriptors.size());
    for (CoreDescriptor descriptor : descriptors) {
      collectionsWithLocalReplica.add(descriptor.getCloudDescriptor().getCollectionName());
    }

    CountDownLatch latch = new CountDownLatch(collectionsWithLocalReplica.size());
    for (String collectionWithLocalReplica : collectionsWithLocalReplica) {
      zkStateReader.registerDocCollectionWatcher(collectionWithLocalReplica, (collectionState) -> {
        if (collectionState == null)  return false;
        boolean foundStates = true;
        for (CoreDescriptor coreDescriptor : descriptors) {
          if (coreDescriptor.getCloudDescriptor().getCollectionName().equals(collectionWithLocalReplica))  {
            Replica replica = collectionState.getReplica(coreDescriptor.getCloudDescriptor().getCoreNodeName());
            if (replica == null || replica.getState() != Replica.State.DOWN) {
              foundStates = false;
            }
          }
        }

        if (foundStates && collectionsWithLocalReplica.remove(collectionWithLocalReplica))  {
          latch.countDown();
        }
        return foundStates;
      });
    }

    boolean allPublishedDown = latch.await(timeoutSeconds, TimeUnit.SECONDS);
    if (!allPublishedDown) {
      log.warn("Timed out waiting to see all nodes published as DOWN in our cluster state.");
    }
  }

  /**
   * Validates if the chroot exists in zk (or if it is successfully created).
   * Optionally, if create is set to true this method will create the path in
   * case it doesn't exist
   *
   * @return true if the path exists or is created false if the path doesn't
   * exist and 'create' = false
   */
  public static boolean checkChrootPath(String zkHost, boolean create)
      throws KeeperException, InterruptedException {
    if (!SolrZkClient.containsChroot(zkHost)) {
      return true;
    }
    log.trace("zkHost includes chroot");
    String chrootPath = zkHost.substring(zkHost.indexOf("/"), zkHost.length());

    SolrZkClient tmpClient = new SolrZkClient(zkHost.substring(0,
        zkHost.indexOf("/")), 60000, 30000, null, null, null);
    boolean exists = tmpClient.exists(chrootPath, true);
    if (!exists && create) {
      tmpClient.makePath(chrootPath, false, true);
      exists = true;
    }
    tmpClient.close();
    return exists;
  }

  public boolean isConnected() {
    return zkClient.isConnected();
  }

  void createEphemeralLiveNode() {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);
    List<Op> ops = new ArrayList<>(2);
    Map<String,byte[]> dataMap = new HashMap<>(2);
    ops.add(Op.create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL));
    dataMap.put(nodePath, null);
    try {
      // if there are nodeAdded triggers don't create nodeAdded markers
      boolean createMarkerNode = zkStateReader.getAutoScalingConfig().hasTriggerForEvents(TriggerEventType.NODEADDED);

      if (createMarkerNode && !zkClient.exists(nodeAddedPath, true)) {
        // use EPHEMERAL so that it disappears if this node goes down
        // and no other action is taken
        byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", TimeSource.NANO_TIME.getEpochTimeNs()));
        ops.add(Op.create(nodeAddedPath, json, zkClient.getZkACLProvider().getACLsToAdd(nodeAddedPath),
            CreateMode.EPHEMERAL));
        dataMap.put(nodeAddedPath, json);
      }



      zkClient.mkDirs(dataMap);
      //zkClient.multi(ops, true);

    } catch (Exception e) {
      throw new SW.Exp(e);
    }
  }

  public void removeEphemeralLiveNode() throws KeeperException, InterruptedException {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    log.info("Remove node as live in ZooKeeper:" + nodePath);
    List<Op> ops = new ArrayList<>(2);
    ops.add(Op.delete(nodePath, -1));
    ops.add(Op.delete(nodeAddedPath, -1));

    try {
      zkClient.multi(ops, true);
    } catch (NoNodeException e) {

    }
  }

  public String getNodeName() {
    assert nodeName != null;
    return nodeName;
  }

  /**
   * Returns true if the path exists
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path, true);
  }


  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc, boolean skipRecovery) throws Exception {
    return register(coreName, desc, false, false, skipRecovery);
  }


  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores,
                         boolean afterExpiration, boolean skipRecovery) throws Exception {
    MDCLoggingContext.setCoreDescriptor(nodeName, desc);
    try {
      // pre register has published our down state
      final String baseUrl = getBaseUrl();
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();
      final String coreZkNodeName = cloudDesc.getCoreNodeName();
      assert coreZkNodeName != null : "we should have a coreNodeName by now";

      // check replica's existence in clusterstate first
      try {
        zkStateReader.waitForState(collection, Overseer.isLegacy(zkStateReader) ? 60000 : 5000,
            TimeUnit.MILLISECONDS, (l, c) -> getReplicaOrNull(c, shardId, coreZkNodeName) != null); // nocommit
      } catch (TimeoutException e) {
        StringBuilder sb = new StringBuilder();
        zkClient.printLayout("/collections", 2, sb);

        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Error registering SolrCore, timeout waiting for replica present in clusterstate, collection=" + collection
                + " shardId=" + shardId + " coreZNodeName=" + coreZkNodeName + " clusterstate=" + sb.toString(),
            e);
      }
      Replica replica = getReplicaOrNull(zkStateReader.getClusterState().getCollectionOrNull(collection), shardId, coreZkNodeName);
      if (replica == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, replica is removed from clusterstate");
      }

      ZkShardTerms shardTerms = getShardTerms(collection, cloudDesc.getShardId());

      if (replica.getType() != Type.PULL) {
        shardTerms.registerTerm(coreZkNodeName);
      }

      if (log.isDebugEnabled())  log.debug("Register replica - core:{} address:{} collection:{} shard:{}", coreName, baseUrl, collection, shardId);
// nocommit
//      try {
//        // If we're a preferred leader, insert ourselves at the head of the queue
//        boolean joinAtHead = replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false);
//        if (replica.getType() != Type.PULL) {
//          joinElection(desc, afterExpiration, joinAtHead);
//        } else if (replica.getType() == Type.PULL) {
//          if (joinAtHead) {
//            log.warn("Replica {} was designated as preferred leader but it's type is {}, It won't join election", coreZkNodeName, Type.PULL);
//          }
//          log.debug("Replica {} skipping election because it's type is {}", coreZkNodeName, Type.PULL);
//          startReplicationFromLeader(coreName, false);
//        }
//      } catch (Exception e) {
//        throw new DW.Exp(e);
//      } 
      joinElection(desc);

      
      // in this case, we want to wait for the leader as long as the leader might
      // wait for a vote, at least - but also long enough that a large cluster has
      // time to get its act together
     // String leaderUrl = getLeader(cloudDesc, leaderVoteWait + 600000); // nocommit timeouts 
      
      //nocommit - need to util this method still
      String leaderRegPath =  ZkStateReader.getShardLeadersPath(collection, shardId);
      CountDownLatch latch = new CountDownLatch(1);
      Stat stat = null;
      try {
        stat = getZkClient().getCurator().checkExists().usingWatcher((CuratorWatcher) event -> {
          
          log.error("EVENT: " + event);
          if (event.getType() == EventType.NodeCreated | event.getType() == EventType.NodeDataChanged) {
            latch.countDown();
          }
        }).forPath(leaderRegPath);
      } catch (Exception e) {
        throw new SW.Exp(e);
      }
      if (stat == null) {
        try {
         boolean success = latch.await(15, TimeUnit.SECONDS);
         if (!success) {
           throw new TimeoutException("Timeout waiting to see registered leader path=" + leaderRegPath);
         }
        } catch (InterruptedException e) {
          SW.propegateInterrupt(e);
        }
      }
      
      //  nocommit - there should be no stale leader state at this point, dont hit zk directly
      String leaderUrl = zkStateReader.getLeaderUrl(collection, shardId);

      String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
      log.info("We are " + ourUrl + " and leader is " + leaderUrl);
      boolean isLeader = leaderUrl.equals(ourUrl);
      assert !(isLeader && replica.getType() == Type.PULL) : "Pull replica became leader!";

      try (SolrCore core = coreAccess.getCore(desc.getName())) {

        // recover from local transaction log and wait for it to complete before
        // going active
        // TODO: should this be moved to another thread? To recoveryStrat?
        // TODO: should this actually be done earlier, before (or as part of)
        // leader election perhaps?

        if (core == null) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCore is no longer available to register");
        }

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        boolean isTlogReplicaAndNotLeader = replica.getType() == Replica.Type.TLOG && !isLeader;
        if (isTlogReplicaAndNotLeader) {
          String commitVersion = ReplicateFromLeader.getCommitVersion(core);
          if (commitVersion != null) {
            ulog.copyOverOldUpdates(Long.parseLong(commitVersion));
          }
        }
        // we will call register again after zk expiration and on reload
        if (!afterExpiration && !core.isReloaded() && ulog != null && !isTlogReplicaAndNotLeader) {
          // disable recovery in case shard is in construction state (for shard splits)
          Slice slice = getClusterState().getCollection(collection).getSlice(shardId);
          if (slice.getState() != Slice.State.CONSTRUCTION || !isLeader) {
            Future<UpdateLog.RecoveryInfo> recoveryFuture = core.getUpdateHandler().getUpdateLog().recoverFromLog();
            if (recoveryFuture != null) {
              log.info("Replaying tlog for " + ourUrl + " during startup... NOTE: This can take a while.");
              recoveryFuture.get(); // NOTE: this could potentially block for
              // minutes or more!
              // TODO: public as recovering in the mean time?
              // TODO: in the future we could do peersync in parallel with recoverFromLog
            } else {
              log.info("No LogReplay needed for core={} baseURL={}", core.getName(), baseUrl);
            }
          }
        }
        boolean didRecovery
            = checkRecovery(recoverReloadedCores, isLeader, skipRecovery, collection, coreZkNodeName, shardId, core, afterExpiration);
        if (!didRecovery) {
          if (isTlogReplicaAndNotLeader) {
            startReplicationFromLeader(coreName, true);
          }
          publish(desc, Replica.State.ACTIVE);
        }

        if (replica.getType() != Type.PULL) {
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          shardTerms.addListener(new RecoveringCoreTermWatcher(this, core.getCoreDescriptor(), coreAccess));
        }
        core.getCoreDescriptor().getCloudDescriptor().setHasRegistered(true);
      } catch (Exception e) {
        Exp exp = new SW.Exp(e);
        try {
          unregister(coreName, desc, false);
        } catch (Exception e1) {
          SW.propegateInterrupt(e1);
          exp.addSuppressed(e1);           
        }
        throw exp;
      }

      // the watcher is added to a set so multiple calls of this method will left only one watcher
      zkStateReader.registerDocCollectionWatcher(cloudDesc.getCollectionName(),
          new UnloadCoreOnDeletedWatcher(coreZkNodeName, shardId, desc.getName()));
      return shardId;
    } finally {
      MDCLoggingContext.clear();
    }
  }

  private Replica getReplicaOrNull(DocCollection docCollection, String shard, String coreNodeName) {
    if (docCollection == null) {
      if (log.isDebugEnabled()) {
        log.debug("getReplicaOrNull#docCollection=null");
      }
      return null;
    }
    Slice slice = docCollection.getSlice(shard);
    if (slice == null) {
      if (log.isDebugEnabled()) {
        log.debug("getReplicaOrNull#shard=null");
      }
      return null;
    }

    Replica replica = slice.getReplica(coreNodeName);
    if (log.isDebugEnabled()) {
      log.debug("getReplicaOrNull#replica=" + replica + " replicas=" + slice.getReplicas());
    }
    return replica;
  }

  public void startReplicationFromLeader(String coreName, boolean switchTransactionLog) throws InterruptedException {
    log.info("{} starting background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = new ReplicateFromLeader(coreAccess, coreName);
    synchronized (replicateFromLeader) { // synchronize to prevent any stop before we finish the start
      if (replicateFromLeaders.putIfAbsent(coreName, replicateFromLeader) == null) {
        replicateFromLeader.startReplication(switchTransactionLog);
      } else {
        log.warn("A replicate from leader instance already exists for core {}", coreName);
      }
    }
  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      synchronized (replicateFromLeader) {
        replicateFromLeader.stopReplication();
      }
    }
  }

  // timeoutms is the timeout for the first call to get the leader - there is then
  // a longer wait to make sure that leader matches our local state

  // timeoutms is the timeout for the first call to get the leader - there is then
  // a longer wait to make sure that leader matches our local state
  private String getLeader(final CloudDescriptor cloudDesc, int timeoutms) {

    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();
    // rather than look in the cluster state file, we go straight to the zknodes
    // here, because on cluster restart there could be stale leader info in the
    // cluster state node that won't be updated for a moment
    String leaderUrl;
    try {
      leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms)
          .getCoreUrl();

      zkStateReader.waitForState(collection, timeoutms * 2, TimeUnit.MILLISECONDS, (n, c) -> checkLeaderUrl(cloudDesc, leaderUrl, collection, shardId, leaderConflictResolveWait));

    } catch (Exception e) {
      throw new SW.Exp("Error getting leader from zk", e);
    }
    return leaderUrl;
  }
  
  private boolean checkLeaderUrl(CloudDescriptor cloudDesc, String leaderUrl, String collection, String shardId,
      int timeoutms) {
    // now wait until our currently cloud state contains the latest leader
    String clusterStateLeaderUrl;
    try {
      clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection, shardId);

     // leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms).getCoreUrl();
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
    return clusterStateLeaderUrl != null;
  }

  /**
   * Get leader props directly from zk nodes.
   * @throws SessionExpiredException on zk session expiration.
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
                                        final String slice, int timeoutms) throws InterruptedException, SessionExpiredException {
    return getLeaderProps(collection, slice, timeoutms, true);
  }

  /**
   * Get leader props directly from zk nodes.
   *
   * @return leader props
   * @throws SessionExpiredException on zk session expiration.
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
      final String slice, int timeoutms, boolean failImmediatelyOnExpiration)
      throws InterruptedException, SessionExpiredException {

//    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
//    Exception exp = null;
//    while (!timeout.hasTimedOut()) {
//      try {
//        getZkStateReader().waitForState(collection, 10, TimeUnit.SECONDS, (n,c) -> c != null && c.getLeader(slice) != null);
//        
//        byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null, true);
//        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
//        return leaderProps;
//
//      } catch (Exception e) {
//        throw new DW.Exp(e);
//      } 
//    }
    CountDownLatch latch = new CountDownLatch(1);
    Stat stat = null;
    try {
      log.info("Check for leader znode");
      stat = getZkClient().getCurator().checkExists().usingWatcher((CuratorWatcher) event -> {

        log.info("Got event on leader node watcher {}", event.toString());
        if (event.getType() == EventType.NodeCreated) {
          if (getZkClient().getCurator().checkExists().forPath(ZkStateReader.getShardLeadersPath(collection, slice)) != null) {
            latch.countDown();
          }
        }

      }).forPath(ZkStateReader.getShardLeadersPath(collection, slice));
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
    if (stat == null) {
      log.info("Shard leader znode not found, waiting on latch");
      try {
        latch.await(); // nocommit timeout
        log.info("Done waiting on latch");
      } catch (InterruptedException e) {
        SW.propegateInterrupt(e);
      }
    }
  
    byte[] data;
    try {
      data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null, true);
    } catch (KeeperException e) {
      throw new SW.Exp(e);
    }
    ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
    return leaderProps;
    
  }

  private void joinElection(CoreDescriptor cd)
      throws InterruptedException, KeeperException, IOException {
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);

    ElectionContext prevContext = electionContexts.get(contextKey);

    if (prevContext != null) {
      prevContext.cancelElection();
    }

    String shardId = cd.getCloudDescriptor().getShardId();

    Map<String, Object> props = new HashMap<>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
    props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    props.put(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);


    ZkNodeProps ourProps = new ZkNodeProps(props);
    
    ShardLeader shardLeader = new ShardLeader(this, ourProps, coreNodeName, collection, shardId);
    
    this.shardLeaders.add(shardLeader);
    
    shardLeader.start();
//
//    LeaderElector leaderElector = new LeaderElector(zkClient, contextKey, electionContexts);
//    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
//        collection, coreNodeName, ourProps, this, cc);
//
//    leaderElector.setup(context);
//    electionContexts.put(contextKey, context);
//    leaderElector.joinElection(context, false, joinAtHead);
  }


  /**
   * Returns whether or not a recovery was started
   */
  private boolean checkRecovery(boolean recoverReloadedCores, final boolean isLeader, boolean skipRecovery,
                                final String collection, String coreZkNodeName, String shardId,
                                SolrCore core, boolean afterExpiration) {
    if (SKIP_AUTO_RECOVERY) {
      log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
      return false;
    }
    boolean doRecovery = true;
    if (!isLeader) {

      if (skipRecovery || (!afterExpiration && core.isReloaded() && !recoverReloadedCores)) {
        doRecovery = false;
      }

      if (doRecovery) {
        log.info("Core needs to recover:" + core.getName());
        core.getUpdateHandler().getSolrCoreState().doRecovery(this, core.getCoreDescriptor());
        return true;
      }

      ZkShardTerms zkShardTerms = getShardTerms(collection, shardId);
      if (zkShardTerms.registered(coreZkNodeName) && !zkShardTerms.canBecomeLeader(coreZkNodeName)) {
        log.info("Leader's term larger than core " + core.getName() + "; starting recovery process");
        core.getUpdateHandler().getSolrCoreState().doRecovery(this, core.getCoreDescriptor());
        return true;
      }
    } else {
      log.info("I am the leader, no recovery necessary");
    }

    return false;
  }


  public String getBaseUrl() {
    return baseURL;
  }

  public void publish(final CoreDescriptor cd, final Replica.State state) throws Exception {
    publish(cd, state, true, false);
  }
  
  public void publish(final SolrCore core, final Replica.State state) throws Exception {
    MDCLoggingContext.setCore(core);
    publish(core.getCoreDescriptor(), state, true, false);
  }
  
  public void publish(final SolrCore core, final Replica.State state, boolean updateLastState, boolean forcePublish) throws Exception {
    MDCLoggingContext.setCore(core);
    publish(core.getCoreDescriptor(), state, updateLastState, forcePublish);
  }

  /**
   * Publish core state to overseer.
   */
  public void publish(final CoreDescriptor cd, final Replica.State state, boolean updateLastState, boolean forcePublish) throws Exception {
    if (!forcePublish) {
      try (SolrCore core = coreAccess.getCore(cd.getName())) {
        MDCLoggingContext.setCore(core);
      }
    } else {
      MDCLoggingContext.setCoreDescriptor(getNodeName(), cd);
    }
    try {
      String collection = cd.getCloudDescriptor().getCollectionName();

      log.debug("publishing state={}", state.toString());
      // System.out.println(Thread.currentThread().getStackTrace()[3]);
      Integer numShards = cd.getCloudDescriptor().getNumShards();
      if (numShards == null) { // XXX sys prop hack
        log.debug("numShards not found on descriptor - reading it from system property");
        numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
      }

      assert collection != null && collection.length() > 0;

      String shardId = cd.getCloudDescriptor().getShardId();

      String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

      Map<String,Object> props = new HashMap<>();
      props.put(Overseer.QUEUE_OPERATION, "state");
      props.put(ZkStateReader.STATE_PROP, state.toString());
      props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
      props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
      props.put(ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles());
      props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
      props.put(ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId());
      props.put(ZkStateReader.COLLECTION_PROP, collection);
      props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().toString());
      if (!Overseer.isLegacy(zkStateReader)) {
        props.put(ZkStateReader.FORCE_SET_STATE_PROP, "false");
      }
      if (numShards != null) {
        props.put(ZkStateReader.NUM_SHARDS_PROP, numShards.toString());
      }
      if (coreNodeName != null) {
        props.put(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
      }
      try (SolrCore core = coreAccess.getCore(cd.getName())) {
        if (state == Replica.State.ACTIVE) {
          ensureRegisteredSearcher(core);
        }
        if (core != null && core.getDirectoryFactory().isSharedStorage()) {
          if (core.getDirectoryFactory().isSharedStorage()) {
            props.put(ZkStateReader.SHARED_STORAGE_PROP, "true");
            props.put("dataDir", core.getDataDir());
            UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
            if (ulog != null) {
              props.put("ulogDir", ulog.getLogDir());
            }
          }
        }
      } catch (SolrCoreInitializationException ex) {
        // The core had failed to initialize (in a previous request, not this one), hence nothing to do here.
        log.info("The core '{}' had failed to initialize before.", cd.getName());
      }

      // pull replicas are excluded because their terms are not considered
      if (state == Replica.State.RECOVERING && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        // state is used by client, state of replica can change from RECOVERING to DOWN without needed to finish recovery
        // by calling this we will know that a replica actually finished recovery or not
        getShardTerms(collection, shardId).startRecovering(coreNodeName);
      }
      if (state == Replica.State.ACTIVE && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        getShardTerms(collection, shardId).doneRecovering(coreNodeName);
      }

      ZkNodeProps m = new ZkNodeProps(props);

      if (updateLastState) {
        cd.getCloudDescriptor().setLastPublished(state);
      }
      sendStateUpdate(m);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public ZkShardTerms getShardTerms(String collection, String shardId) {
    return getCollectionTerms(collection).getShard(shardId);
  }

  private ZkCollectionTerms getCollectionTerms(String collection) {
    synchronized (collectionToTerms) {
      if (!collectionToTerms.containsKey(collection)) collectionToTerms.put(collection, new ZkCollectionTerms(collection, zkClient));
      return collectionToTerms.get(collection);
    }
  }

  public void clearZkCollectionTerms() {
    synchronized (collectionToTerms) {
      collectionToTerms.values().forEach(ZkCollectionTerms::close);
      collectionToTerms.clear();
    }
  }

  public void unregister(String coreName, CoreDescriptor cd) throws Exception {
    unregister(coreName, cd, true);
  }

  public void unregister(String coreName, CoreDescriptor cd, boolean removeCoreFromZk) throws Exception {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    final String collection = cd.getCloudDescriptor().getCollectionName();
    getCollectionTerms(collection).remove(cd.getCloudDescriptor().getShardId(), cd);
    replicasMetTragicEvent.remove(collection+":"+coreNodeName);

    if (Strings.isNullOrEmpty(collection)) {
      log.error("No collection was specified.");
      assert false : "No collection was specified [" + collection + "]";
      return;
    }
    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    Replica replica = (docCollection == null) ? null : docCollection.getReplica(coreNodeName);

    if (replica == null || replica.getType() != Type.PULL) {
      ElectionContext context = electionContexts.remove(new ContextKey(collection, coreNodeName));

      if (context != null) {
        context.cancelElection();
      }
    }
    CloudDescriptor cloudDescriptor = cd.getCloudDescriptor();
    if (removeCoreFromZk) {
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          OverseerAction.DELETECORE.toLower(), ZkStateReader.CORE_NAME_PROP, coreName,
          ZkStateReader.NODE_NAME_PROP, getNodeName(),
          ZkStateReader.COLLECTION_PROP, cloudDescriptor.getCollectionName(),
          ZkStateReader.BASE_URL_PROP, getBaseUrl(),
          ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
      sendStateUpdate(m);
    }
  }

  public void createCollection(String collection) throws Exception {
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        CollectionParams.CollectionAction.CREATE.toLower(), ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, collection);
    sendStateUpdate(m);
  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  private void doGetShardIdAndNodeNameProcess(CoreDescriptor cd) {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    if (coreNodeName != null) {
      waitForShardId(cd);
    } else {
      // if no explicit coreNodeName, we want to match by base url and core name
      waitForCoreNodeName(cd);
      waitForShardId(cd);
    }
  }

  private void waitForCoreNodeName(CoreDescriptor cd) {
    if (log.isDebugEnabled()) log.debug("look for our core node name");

    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      zkStateReader.waitForState(cd.getCollectionName(), 320, TimeUnit.SECONDS, (n, c) -> {
        if (c == null)
          return false;
        final Map<String,Slice> slicesMap = c.getSlicesMap();
        if (slicesMap == null) {
          return false;
        }
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            String msgNodeName = getNodeName();
            String msgCore = cd.getName();

            if (msgNodeName.equals(nodeName) && core.equals(msgCore)) {
              cd.getCloudDescriptor()
                  .setCoreNodeName(replica.getName());
              return true;
            }
          }
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName() + " " + error);
    }
  }
  
  private void waitForShardId(CoreDescriptor cd) {
    if (log.isDebugEnabled()) {
      log.debug("waitForShardId(CoreDescriptor cd={}) - start", cd);
    }

    AtomicReference<String> returnId = new AtomicReference<>();
    try {
      try {
        zkStateReader.waitForState(cd.getCollectionName(), 5, TimeUnit.SECONDS, (n, c) -> { // nocommit
            if (c == null) return false;
            String shardId = c.getShardId(cd.getCloudDescriptor().getCoreNodeName());
            if (shardId != null) {
              returnId.set(shardId);
              return true;
            }
            return false;
        });
      } catch (InterruptedException e) {
        throw new SW.Exp(e);
      }
    } catch (TimeoutException e1) {
      log.error("waitForShardId(CoreDescriptor=" + cd + ")", e1);

      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName());
    }

    final String shardId = returnId.get();
    if (shardId != null) {
      cd.getCloudDescriptor().setShardId(shardId);

      if (log.isDebugEnabled()) {
        log.debug("waitForShardId(CoreDescriptor) - end coreNodeName=" + cd.getCloudDescriptor().getCoreNodeName() + " shardId=" + shardId);
      }
      return;
    }

    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName());
  }

  public String getCoreNodeName(CoreDescriptor descriptor) {
    String coreNodeName = descriptor.getCloudDescriptor().getCoreNodeName();
    if (coreNodeName == null && !genericCoreNodeNames) {
      // it's the default
      return getNodeName() + "_" + descriptor.getName();
    }

    return coreNodeName;
  }

  public void preRegister(CoreDescriptor cd, boolean publishState) {

    String coreNodeName = getCoreNodeName(cd);

    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    try {
      checkStateInZk(cd);

      CloudDescriptor cloudDesc = cd.getCloudDescriptor();

      // make sure the node name is set on the descriptor
      if (cloudDesc.getCoreNodeName() == null) {
        cloudDesc.setCoreNodeName(coreNodeName);
      }

      // publishState == false on startup
      if (publishState || isPublishAsDownOnStartup(cloudDesc)) {
        publish(cd, Replica.State.DOWN, false, true);
      }
      String collectionName = cd.getCloudDescriptor().getCollectionName();
      DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
      log.debug(collection == null ?
              "Collection {} not visible yet, but flagging it so a watch is registered when it becomes visible" :
              "Registering watch for collection {}",
          collectionName);
    } catch (Exception e) {
      throw new SW.Exp(e);
    } 

    doGetShardIdAndNodeNameProcess(cd);

  }

  /**
   * On startup, the node already published all of its replicas as DOWN,
   * so in case of legacyCloud=false ( the replica must already present on Zk )
   * we can skip publish the replica as down
   * @return Should publish the replica as down on startup
   */
  private boolean isPublishAsDownOnStartup(CloudDescriptor cloudDesc) {
    if (!Overseer.isLegacy(zkStateReader)) {
      Replica replica = zkStateReader.getClusterState().getCollection(cloudDesc.getCollectionName())
          .getSlice(cloudDesc.getShardId())
          .getReplica(cloudDesc.getCoreNodeName());
      if (replica.getNodeName().equals(getNodeName())) {
        return false;
      }
    }
    return true;
  }

  private void checkStateInZk(CoreDescriptor cd) throws InterruptedException, NotInClusterStateException {
    CloudDescriptor cloudDesc = cd.getCloudDescriptor();
    String coreNodeName = cloudDesc.getCoreNodeName();
    
    if (!Overseer.isLegacy(zkStateReader)) {

      if (coreNodeName == null) {
        // nocommit wtf
        throw new SolrException(ErrorCode.SERVER_ERROR, "WTF");
        // if (cc.repairCoreProperty(cd, CoreDescriptor.CORE_NODE_NAME) == false) {
        // throw new SolrException(ErrorCode.SERVER_ERROR, "No coreNodeName for " + cd);
        // }
        // nodeName = cloudDesc.getCoreNodeName();
        // // verify that the repair worked.
        // if (nodeName == null) {
        // throw new SolrException(ErrorCode.SERVER_ERROR, "No coreNodeName for " + cd);
      }
    }

    if (cloudDesc.getShardId() == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "No shard id for " + cd);
    }

    AtomicReference<String> errorMessage = new AtomicReference<>();
    AtomicReference<DocCollection> collectionState = new AtomicReference<>();
    try {
      zkStateReader.waitForState(cd.getCollectionName(), 10, TimeUnit.SECONDS, (c) -> {
        collectionState.set(c);
        if (c == null)
          return false;
        Slice slice = c.getSlice(cloudDesc.getShardId());
        if (slice == null) {
          errorMessage.set("Invalid shard: " + cloudDesc.getShardId());
          return false;
        }
        Replica replica = slice.getReplica(coreNodeName);
        if (replica == null) {
          errorMessage.set("coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
              ", ignore the exception if the replica was deleted: " + c);
          return false;
        }
        return true;
      });
    } catch (TimeoutException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
            ", ignore the exception if the replica was deleted";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }
  }
  

  public static void linkConfSet(SolrZkClient zkClient, String collection, String confSetName) throws KeeperException, InterruptedException {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Load collection config from:" + path);
    byte[] data;
    try {
      data = zkClient.getData(path, null, null, true);
    } catch (NoNodeException e) {
      // if there is no node, we will try and create it
      // first try to make in case we are pre configuring
      ZkNodeProps props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
      try {

        zkClient.makePath(path, Utils.toJSON(props),
            CreateMode.PERSISTENT, null, true);
      } catch (KeeperException e2) {
        // it's okay if the node already exists
        if (e2.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
        // if we fail creating, setdata
        // TODO: we should consider using version
        zkClient.setData(path, Utils.toJSON(props), true);
      }
      return;
    }
    // we found existing data, let's update it
    ZkNodeProps props = null;
    if (data != null) {
      props = ZkNodeProps.load(data);
      Map<String, Object> newProps = new HashMap<>(props.getProperties());
      newProps.put(CONFIGNAME_PROP, confSetName);
      props = new ZkNodeProps(newProps);
    } else {
      props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
    }

    // TODO: we should consider using version
    zkClient.setData(path, Utils.toJSON(props), true);

  }

  /**
   * If in SolrCloud mode, upload config sets for each SolrCore in solr.xml.
   */
  public static void bootstrapConf(SolrZkClient zkClient, CoreContainer cc, String solrHome) throws IOException {

    ZkConfigManager configManager = new ZkConfigManager(zkClient);

    //List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);

    log.info("bootstrapping config for " + cds.size() + " cores into ZooKeeper using solr.xml from " + solrHome);

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName))
        confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory " + udir + " with name " + confName + " for SolrCore " + coreName);
      configManager.uploadConfigDir(udir, confName);
    }
  }

//  public ZkDistributedQueue getOverseerJobQueue() {
//    return overseerJobQueue;
//  }

  public OverseerTaskQueue getOverseerCollectionQueue() {
    return overseerCollectionQueue;
  }

  public OverseerTaskQueue getOverseerConfigSetQueue() {
    return overseerConfigSetQueue;
  }

  public DistributedMap getOverseerRunningMap() {
    return overseerRunningMap;
  }

  public DistributedMap getOverseerCompletedMap() {
    return overseerCompletedMap;
  }

  public DistributedMap getOverseerFailureMap() {
    return overseerFailureMap;
  }

  /**
   * When an operation needs to be performed in an asynchronous mode, the asyncId needs
   * to be claimed by calling this method to make sure it's not duplicate (hasn't been
   * claimed by other request). If this method returns true, the asyncId in the parameter
   * has been reserved for the operation, meaning that no other thread/operation can claim
   * it. If for whatever reason, the operation is not scheduled, the asuncId needs to be
   * cleared using {@link #clearAsyncId(String)}.
   * If this method returns false, no reservation has been made, and this asyncId can't
   * be used, since it's being used by another operation (currently or in the past)
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the reservation succeeds.
   *         False if this ID is already in use.
   */
  public boolean claimAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.putIfAbsent(asyncId, new byte[0]);
    } catch (InterruptedException e) {
      log.error("Could not claim asyncId=" + asyncId, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Clears an asyncId previously claimed by calling {@link #claimAsyncId(String)}
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the asyncId existed and was cleared.
   *         False if the asyncId didn't exist before.
   */
  public boolean clearAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.remove(asyncId);
    } catch (InterruptedException e) {
      log.error("Could not release asyncId=" + asyncId, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  public int getClientTimeout() {
    return clientTimeout;
  }

  public Overseer getOverseer() {
    return overseer;
  }

  public LeaderElector getOverseerElector() {
    return overseerElector;
  }

  /**
   * Returns the nodeName that should be used based on the specified properties.
   *
   * @param hostName    - must not be null or the empty string
   * @param hostPort    - must consist only of digits, must not be null or the empty string
   * @param hostContext - should not begin or end with a slash (leading/trailin slashes will be ignored), must not be null, may be the empty string to denote the root context
   * @lucene.experimental
   * @see ZkStateReader#getBaseUrlForNodeName
   */
  static String generateNodeName(final String hostName,
                                 final String hostPort,
                                 final String hostContext) {
    try {
      return hostName + ':' + hostPort + '_' +
          URLEncoder.encode(trimLeadingAndTrailingSlashes(hostContext), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error("JVM Does not seem to support UTF-8", e);
    }
  }

  /**
   * Utility method for trimming and leading and/or trailing slashes from
   * its input.  May return the empty string.  May return null if and only
   * if the input is null.
   */
  public static String trimLeadingAndTrailingSlashes(final String in) {
    if (null == in) return in;

    String out = in;
    if (out.startsWith("/")) {
      out = out.substring(1);
    }
    if (out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  public void rejoinOverseerElection(String electionNode, boolean joinAtHead) {
    try {
      if (electionNode != null) {
        // Check whether we came to this node by mistake
        if ( overseerElector.getContext() != null && overseerElector.getContext().leaderSeqPath == null 
            && !overseerElector.getContext().leaderSeqPath.endsWith(electionNode)) {
          log.warn("Asked to rejoin with wrong election node : {}, current node is {}", electionNode, overseerElector.getContext().leaderSeqPath);
          //however delete it . This is possible when the last attempt at deleting the election node failed.
          if (electionNode.startsWith(getNodeName())) {
            try {
              zkClient.delete(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE + "/" + electionNode, -1, true);
            } catch (NoNodeException e) {
              //no problem
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              log.warn("Old election node exists , could not be removed ", e);
            }
          }
        } else { // We're in the right place, now attempt to rejoin
          overseerElector.retryElection(new OverseerElectionContext(getNodeName(), zkClient, overseer), joinAtHead);
          return;
        }
      } else {
        overseerElector.retryElection(overseerElector.getContext(), joinAtHead);
      }
    } catch (Exception e) {
      throw new SW.Exp("Unable to rejoin election", e);
    }

  }

//  public void rejoinShardLeaderElection(SolrParams params) {
//    try {
//
//      String collectionName = params.get(COLLECTION_PROP);
//      String shardId = params.get(SHARD_ID_PROP);
//      String coreNodeName = params.get(CORE_NODE_NAME_PROP);
//      String coreName = params.get(CORE_NAME_PROP);
//      String electionNode = params.get(ELECTION_NODE_PROP);
//      String baseUrl = params.get(BASE_URL_PROP);
//
//      try (SolrCore core = cc.getCore(coreName)) {
//        MDCLoggingContext.setCore(core);
//
//        log.info("Rejoin the shard leader election.");
//
//        ContextKey contextKey = new ContextKey(collectionName, coreNodeName);
//
//        ElectionContext prevContext = electionContexts.get(contextKey);
//        if (prevContext != null) prevContext.cancelElection();
//
//        ZkNodeProps zkProps = new ZkNodeProps(BASE_URL_PROP, baseUrl, CORE_NAME_PROP, coreName, NODE_NAME_PROP, getNodeName(), CORE_NODE_NAME_PROP, coreNodeName);
//
////        LeaderElector elect = ((ShardLeaderElectionContext) prevContext).getLeaderElector();
////        ShardLeaderElectionContext context = new ShardLeaderElectionContext(elect, shardId, collectionName,
////            coreNodeName, zkProps, this, getCoreContainer());
//
//        context.leaderSeqPath = context.electionPath + LeaderElector.ELECTION_NODE + "/" + electionNode;
//        elect.setup(context);
//        electionContexts.put(contextKey, context);
//
//        elect.retryElection(context, params.getBool(REJOIN_AT_HEAD_PROP, false));
//      }
//    } catch (Exception e) {
//      throw new DW.Exp("Unable to rejoin election", e);
//    }
//
//  }

  // nocommit - overseer changing and lots not do 1000 requests at startup
//  public void checkOverseerDesignate() {
//    try {
//      byte[] data = zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true);
//      if (data == null) return;
//      Map roles = (Map) Utils.fromJSON(data);
//      if (roles == null) return;
//      List nodeList = (List) roles.get("overseer");
//      if (nodeList == null) return;
//      if (nodeList.contains(getNodeName())) {
//        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.ADDROLE.toString().toLowerCase(Locale.ROOT),
//            "node", getNodeName(),
//            "role", "overseer");
//        log.info("Going to add role {} ", props);
//        getOverseerCollectionQueue().offer(Utils.toJSON(props));
//      }
//    } catch (NoNodeException nne) {
//      return;
//    } catch (Exception e) {
//      throw new DW.Exp("could not read the overseer designate ", e);
//    }
//  

  public CoreContainer getCoreContainer() {
    return cc;
  }
  
  /**
   * Add a listener to be notified once there is a new session created after a ZooKeeper session expiration occurs;
   * in most cases, listeners will be components that have watchers that need to be re-created.
   */
  public void addOnReconnectListener(OnReconnect listener) { // nocommit fix sync
    if (listener != null) {
      synchronized (reconnectListeners) {
        reconnectListeners.add(listener);
        log.debug("Added new OnReconnect listener "+listener);
      }
    }
  }

  /**
   * Removed a previously registered OnReconnect listener, such as when a core is removed or reloaded.
   */
  public void removeOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
      boolean wasRemoved;
      synchronized (reconnectListeners) {
        wasRemoved = reconnectListeners.remove(listener);
      }
      if (wasRemoved) {
        log.debug("Removed OnReconnect listener "+listener);
      } else {
        log.warn("Was asked to remove OnReconnect listener "+listener+
            ", but remove operation did not find it in the list of registered listeners.");
      }
    }
  }

  Set<OnReconnect> getCurrentOnReconnectListeners() {
    return Collections.unmodifiableSet(reconnectListeners);
  }

  /**
   * Persists a config file to ZooKeeper using optimistic concurrency.
   *
   * @return true on success
   */
  public static int persistConfigResourceToZooKeeper(ZkSolrResourceLoader zkLoader, int znodeVersion,
                                                         String resourceName, byte[] content,
                                                         boolean createIfNotExists) {
    int latestVersion = znodeVersion;
    final ZkController zkController = zkLoader.getZkController();
    final SolrZkClient zkClient = zkController.getZkClient();
    final String resourceLocation = zkLoader.getConfigSetZkPath() + "/" + resourceName;
    String errMsg = "Failed to persist resource at {0} - old {1}";
    try {
      try {
        Stat stat = zkClient.setData(resourceLocation, content, znodeVersion, true);
        latestVersion = stat.getVersion();// if the set succeeded , it should have incremented the version by one always
        log.info("Persisted config data to node {} ", resourceLocation);
        touchConfDir(zkLoader);
      } catch (NoNodeException e) {
        if (createIfNotExists) {
          try {
            zkClient.create(resourceLocation, content, CreateMode.PERSISTENT, true);
            latestVersion = 0;//just created so version must be zero
            touchConfDir(zkLoader);
          } catch (KeeperException.NodeExistsException nee) {
            try {
              Stat stat = zkClient.exists(resourceLocation, null, true);
              log.debug("failed to set data version in zk is {} and expected version is {} ", stat.getVersion(), znodeVersion);
            } catch (Exception e1) {
              throw new SW.Exp(e1);
            }

            log.info(StrUtils.formatString(errMsg, resourceLocation, znodeVersion));
            throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
          }
        }
      }

    } catch (KeeperException.BadVersionException bve) {
      int v = -1;
      try {
        Stat stat = zkClient.exists(resourceLocation, null, true);
        v = stat.getVersion();
      } catch (Exception e) {
        SW.propegateInterrupt(e);
      }
      log.info(StrUtils.formatString(errMsg + " zkVersion= " + v, resourceLocation, znodeVersion));
      throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
    } catch (ResourceModifiedInZkException e) {
      throw e;
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
    return latestVersion;
  }

  public static void touchConfDir(ZkSolrResourceLoader zkLoader) {
    SolrZkClient zkClient = zkLoader.getZkController().getZkClient();
    try {
      zkClient.setData(zkLoader.getConfigSetZkPath(), new byte[]{0}, true);
    } catch (Exception e) {
      throw new SW.Exp("Error 'touching' conf location " + zkLoader.getConfigSetZkPath(), e);
    }
  }

  public static class ResourceModifiedInZkException extends SolrException {
    public ResourceModifiedInZkException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  private void unregisterConfListener(String confDir, Runnable listener) {

    final Set<Runnable> listeners = confDirectoryListeners.get(confDir);
    if (listeners == null) {
      log.warn(confDir + " has no more registered listeners, but a live one attempted to unregister!");
      return;
    }
    if (listeners.remove(listener)) {
      log.debug("removed listener for config directory [{}]", confDir);
    }
    if (listeners.isEmpty()) {
      // no more listeners for this confDir, remove it from the map
      log.debug("No more listeners for config directory [{}]", confDir);
      confDirectoryListeners.remove(confDir);
    }

  }

  /**
   * This will give a callback to the listener whenever a child is modified in the
   * conf directory. It is the responsibility of the listener to check if the individual
   * item of interest has been modified.  When the last core which was interested in
   * this conf directory is gone the listeners will be removed automatically.
   */
  public void registerConfListenerForCore(final String confDir, SolrCore core, final Runnable listener) {
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }
    synchronized (confDirectoryListeners) {
      final Set<Runnable> confDirListeners = getConfDirListeners(confDir);
      confDirListeners.add(listener);
      core.addCloseHook(new CloseHook() {
        @Override
        public void preClose(SolrCore core) {
          unregisterConfListener(confDir, listener);
        }

        @Override
        public void postClose(SolrCore core) {
        }
      });
    }
  }

  // this method is called in a protected confDirListeners block
  private Set<Runnable> getConfDirListeners(final String confDir) {
    synchronized (confDirectoryListeners) {
      Set<Runnable> confDirListeners = confDirectoryListeners.get(confDir);
      if (confDirListeners == null) {
        if (log.isDebugEnabled()) log.debug("watch zkdir {}", confDir);
        confDirListeners = SW.concSetSmallO();
        confDirectoryListeners.put(confDir, confDirListeners);
        setConfWatcher(confDir, new WatcherImpl(confDir), null);
      }
      return confDirListeners;
    }
  }

  private final Map<String, Set<Runnable>> confDirectoryListeners = SW.concMapSmallO();

  private class WatcherImpl implements Watcher {
    private final String zkDir;

    private WatcherImpl(String dir) {
      this.zkDir = dir;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      Stat stat = null;
      try {
        stat = zkClient.exists(zkDir, null, true);
      } catch (Exception e) {
        throw new SW.Exp(e);
      }

      boolean resetWatcher = false;
      try {
        resetWatcher = fireEventListeners(zkDir);
      } finally {
        if (Event.EventType.None.equals(event.getType())) {
          if (log.isDebugEnabled()) log.debug("A node got unwatched for {}", zkDir);
        } else {
          if (resetWatcher) setConfWatcher(zkDir, this, stat);
          else if (log.isDebugEnabled()) log.debug("A node got unwatched for {}", zkDir);
        }
      }
    }
  }

  private boolean fireEventListeners(String zkDir) {
    if (isClosed) {
      return false;
    }
    synchronized (confDirectoryListeners) {
      // if this is not among directories to be watched then don't set the watcher anymore
      if (!confDirectoryListeners.containsKey(zkDir)) {
        log.debug("Watcher on {} is removed ", zkDir);
        return false;
      }
    }
    final Set<Runnable> listeners = confDirectoryListeners.get(zkDir);
    if (listeners != null) {

      // run these in a separate thread because this can be long running

      try (SW worker = new SW(this, true)) {
        listeners.forEach((it) -> worker.collect(() -> {
          it.run();
          return it;
        }));

      }
    }
    return true;
  }

  private void setConfWatcher(String zkDir, Watcher watcher, Stat stat) {
    try {
      Stat newStat = zkClient.exists(zkDir, watcher, true);
      if (stat != null && newStat.getVersion() > stat.getVersion()) {
        //a race condition where a we missed an event fired
        //so fire the event listeners
        fireEventListeners(zkDir);
      }
    } catch (Exception e) {
      throw new SW.Exp("failed to set watcher for conf dir" + zkDir, e);
    } 
  }

  public OnReconnect getConfigDirListener() {
    return () -> {
      synchronized (confDirectoryListeners) {
        for (String s : confDirectoryListeners.keySet()) {
          setConfWatcher(s, new WatcherImpl(s), null);
          fireEventListeners(s);
        }
      }
    };
  }

  /** @lucene.internal */
  class UnloadCoreOnDeletedWatcher implements DocCollectionWatcher {
    String coreNodeName;
    String shard;
    String coreName;

    public UnloadCoreOnDeletedWatcher(String coreNodeName, String shard, String coreName) {
      this.coreNodeName = coreNodeName;
      this.shard = shard;
      this.coreName = coreName;
    }

    @Override
    // nocommit - wow this thing is scary.
    public synchronized boolean onStateChanged(DocCollection collectionState) {
      if (isClosed) { // don't accidently delete cores on shutdown due to unreliable state
        return true;
      }
      
//      if (getCoreContainer().getCoreDescriptor(coreName) == null) return true;
//
//      boolean removeReplica = shouldUnloadReplica(collectionState, shard, coreNodeName);
//      if (removeReplica) {
//        try {
//          log.info("Replica {} removed from clusterstate, remove it.", coreName, collectionState);
//          getCoreContainer().unload(coreName, true, true, true);
//        } catch (Exception e) {
//          throw new DW.Exp("Failed to unregister core: " + coreName, e);
//        }
//      }
//      return removeReplica;
      return true;
    }
    
    private boolean shouldUnloadReplica(DocCollection docCollection, String shard, String coreNodeName) {
//      if (docCollection == null) return false; // we have to be conservative or we get it wrong - only unload if collection and slice exist
//
//      Slice slice = docCollection.getSlice(shard);
//      if (slice == null) return false;
//
//      Replica replica = slice.getReplica(coreNodeName);
//      if (replica == null) return true;
//      if (!getNodeName().equals(replica.getNodeName())) return true;

      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UnloadCoreOnDeletedWatcher that = (UnloadCoreOnDeletedWatcher) o;
      return Objects.equals(coreNodeName, that.coreNodeName) &&
          Objects.equals(shard, that.shard) &&
          Objects.equals(coreName, that.coreName);
    }

    @Override
    public int hashCode() {

      return Objects.hash(coreNodeName, shard, coreName);
    }
  }

  /**
   * Thrown during pre register process if the replica is not present in clusterstate
   */
  public static class NotInClusterStateException extends SolrException {
    public NotInClusterStateException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  public boolean checkIfCoreNodeNameAlreadyExists(CoreDescriptor dcore) {
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(dcore.getCollectionName());
    if (collection != null) {
      Collection<Slice> slices = collection.getSlices();

      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        Replica r = slice.getReplica(dcore.getCloudDescriptor().getCoreNodeName());
        if (r != null) {
          return true;
        }
      }
    }
    return false;
  }


  /**
   * Best effort to set DOWN state for all replicas on node.
   *
   * @param nodeName to operate on
   */
  public void publishNodeAsDown(String nodeName) {
    log.info("Publish node={} as DOWN", nodeName);
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.DOWNNODE.toLower(),
        ZkStateReader.NODE_NAME_PROP, nodeName);
    solrSeer.sendUpdate(m);
//      try {
//        overseer.offerStateUpdate(m);
//      } catch (Exception e) {
//        throw new DW.Exp(e);
//      }
//     
  }

  /**
   * Ensures that a searcher is registered for the given core and if not, waits until one is registered
   */
  private static void ensureRegisteredSearcher(SolrCore core) throws InterruptedException {
    if (!core.getSolrConfig().useColdSearcher) {

      if (core.hasRegisteredSearcher()) {
        if (log.isDebugEnabled()) {
          log.debug("Found a registered searcher for core: {}", core);
        }

        return;
      }

      Future[] waitSearcher = new Future[1];
      log.info(
          "No registered searcher found for core: {}, waiting until a searcher is registered before publishing as active",
          core.getName());
      final RTimer timer = new RTimer();

      core.getSearcher(false, false, waitSearcher, true);
      boolean success = false;
      if (waitSearcher[0] != null) {
        if (log.isDebugEnabled()) {
          log.debug("Waiting for first searcher of core {}, id: {} to be registered", core.getName(), core);
        }
        try {
          waitSearcher[0].get();
          success = true;
        } catch (Exception e) {
          throw new SW.Exp(
              "Wait for a searcher to be registered for core " + core.getName() + ", id: " + core + " failed due to: ",
              e);
        }
      }
      if (success) {
        log.info("Found a registered searcher, took: {} ms for core: {}, id: {}", timer.getTime(), core.getName(),
            core);
      }

    }

  }

  public void sendStateUpdate(ZkNodeProps msg) throws Exception {
    solrSeer.sendUpdate(msg);
  }

  public void sendAdminOp(ZkNodeProps m) {
    solrSeer.sendAdminUpdate(m);
  }


  public CoreAccess getCoreAccess() {
    return coreAccess;
  }
  
}
