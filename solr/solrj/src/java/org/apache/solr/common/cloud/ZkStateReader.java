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
package org.apache.solr.common.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.cloud.CloudInspectUtil;
import org.apache.solr.cloud.DoNotWrap;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.Callable;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.emptySortedSet;
import static org.apache.solr.common.util.Utils.fromJSON;

public class ZkStateReader implements SolrCloseable, Replica.NodeNameToBaseUrl {
  public static final int STATE_UPDATE_DELAY = Integer.getInteger("solr.OverseerStateUpdateDelay", 2000);  // delay between cloud state updates
  public static final String STRUCTURE_CHANGE_NOTIFIER = "_scn";
  public static final String STATE_UPDATES = "_statupdates";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final byte[] emptyJson = Utils.toJSON(EMPTY_MAP);

  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";

  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";

  /**
   * SolrCore name.
   */
  public static final String CORE_NAME_PROP = "core";
  public static final String COLLECTION_PROP = "collection";
  public static final String ELECTION_NODE_PROP = "election_node";
  public static final String SHARD_ID_PROP = "shard";
  public static final String REPLICA_PROP = "replica";
  public static final String SHARD_RANGE_PROP = "shard_range";
  public static final String SHARD_STATE_PROP = "shard_state";
  public static final String SHARD_PARENT_PROP = "shard_parent";
  public static final String NUM_SHARDS_PROP = "numShards";
  public static final String LEADER_PROP = "leader";
  public static final String SHARED_STORAGE_PROP = "shared_storage";
  public static final String PROPERTY_PROP = "property";
  public static final String PROPERTY_PROP_PREFIX = "property.";
  public static final String PROPERTY_VALUE_PROP = "property.value";
  public static final String MAX_AT_ONCE_PROP = "maxAtOnce";
  public static final String MAX_WAIT_SECONDS_PROP = "maxWaitSeconds";
  public static final String STATE_TIMESTAMP_PROP = "stateTimestamp";
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String ALIASES = "/aliases.json";
  public static final String STATE_JSON = "/state.json";
  /**
   * This ZooKeeper file is no longer used starting with Solr 9 but keeping the name around to check if it
   * is still present and non empty (in case of upgrade from previous Solr version). It used to contain collection
   * state for all collections in the cluster.
   */
  public static final String UNSUPPORTED_CLUSTER_STATE = "/clusterstate.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String COLLECTION_PROPS_ZKNODE = "collectionprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";
  public static final String SOLR_PKGS_PATH = "/packages.json";

  public static final String DEFAULT_SHARD_PREFERENCES = "defaultShardPreferences";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String MAX_CORES_PER_NODE = "maxCoresPerNode";
  public static final String PULL_REPLICAS = "pullReplicas";
  public static final String NRT_REPLICAS = "nrtReplicas";
  public static final String TLOG_REPLICAS = "tlogReplicas";
  public static final String READ_ONLY = "readOnly";

  public static final String ROLES = "/roles.json";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP = "configName";

  public static final String SAMPLE_PERCENTAGE = "samplePercentage";

  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionAdminParams#DEFAULTS} instead.
   */
  @Deprecated
  public static final String COLLECTION_DEF = "collectionDefaults";

  public static final String URL_SCHEME = "urlScheme";

  private static final String SOLR_ENVIRONMENT = "environment";

  public static final String REPLICA_TYPE = "type";

  private CloseTracker closeTracker;


  private static class AllCollections {

    private final ConcurrentHashMap<String,DocCollection> watchedCollectionStates;
    private final ConcurrentHashMap<String,ClusterState.CollectionRef> lazyCollectionStates;

    AllCollections(ConcurrentHashMap<String, DocCollection> watchedCollectionStates, ConcurrentHashMap<String, ClusterState.CollectionRef> lazyCollectionStates) {
      this.watchedCollectionStates = watchedCollectionStates;
      this.lazyCollectionStates = lazyCollectionStates;
    }

    public int size() {
      return watchedCollectionStates.size() + lazyCollectionStates.size();
    }

    public ClusterState.CollectionRef get(String collection) {
      DocCollection docCollection = watchedCollectionStates.get(collection);
      if (docCollection != null) {
        return new ClusterState.CollectionRef(docCollection);
      }
      ClusterState.CollectionRef lazyRef = lazyCollectionStates.get(collection);
      if (lazyRef != null) {
        return lazyRef;
      }
      return null;
    }

    public Set<String> keySet() {
      Set keySet = new HashSet(watchedCollectionStates.size() + lazyCollectionStates.size());
      keySet.addAll(watchedCollectionStates.keySet());
      keySet.addAll(lazyCollectionStates.keySet());
      return keySet;
    }
  }

  private final int GET_LEADER_RETRY_DEFAULT_TIMEOUT = Integer.parseInt(System.getProperty("zkReaderGetLeaderRetryTimeoutMs", "1000"));

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";



  /**
   * "Interesting" and actively watched Collections.
   */
  private final ConcurrentHashMap<String, DocCollection> watchedCollectionStates = new ConcurrentHashMap<>(128, 0.75f, 8);

  private final ConcurrentHashMap<String, Map> stateUpdates = new ConcurrentHashMap<>(128, 0.75f, 8);

  /**
   * "Interesting" but not actively watched Collections.
   */
  private final ConcurrentHashMap<String,ClusterState.CollectionRef> lazyCollectionStates = new ConcurrentHashMap<>(128, 0.75f, 8);

  private final AllCollections allCollections = new AllCollections(watchedCollectionStates, lazyCollectionStates);

  /**
   * Collection properties being actively watched
   */
  private final ConcurrentHashMap<String, VersionedCollectionProps> watchedCollectionProps = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, ReentrantLock> collectionStateLocks = new ConcurrentHashMap<>(128, 0.75f, 8);

  /**
   * Watchers of Collection properties
   */
  private final ConcurrentHashMap<String, PropsWatcher> collectionPropsWatchers = new ConcurrentHashMap<>();

  private volatile SortedSet<String> liveNodes = emptySortedSet();

  private volatile int liveNodesVersion = -1;

  private final ReentrantLock liveNodesLock = new ReentrantLock(true);

  private final ReentrantLock clusterStateLock = new ReentrantLock(true);

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();

  private final ZkConfigManager configManager;

  private ConfigData securityData;

  private final Runnable securityNodeListener;

  private final ConcurrentHashMap<String, CollectionWatch<DocCollectionWatcher>> collectionWatches = new ConcurrentHashMap<>(128, 0.75f, 8);

  private Set<String> registeredCores = ConcurrentHashMap.newKeySet();

  private final Map<String,CollectionStateWatcher> stateWatchersMap = new ConcurrentHashMap<>(128, 0.75f, 8);

  // named this observers so there's less confusion between CollectionPropsWatcher map and the PropsWatcher map.
  private final ConcurrentHashMap<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers = new ConcurrentHashMap<>();

  private Set<CloudCollectionsListener> cloudCollectionsListeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService notifications = ParWork.getExecutorService(Integer.MAX_VALUE, false, false);

  private final Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();

  private final Set<ClusterPropertiesListener> clusterPropertiesListeners = ConcurrentHashMap.newKeySet();

  private static final long LAZY_CACHE_TIME = TimeUnit.NANOSECONDS.convert(STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  private volatile Future<?> collectionPropsCacheCleaner; // only kept to identify if the cleaner has already been started.
  private volatile String node = null;
  private volatile LiveNodeWatcher liveNodesWatcher;
  private volatile CollectionsChildWatcher collectionsChildWatcher;
  private volatile IsLocalLeader isLocalLeader;
  private volatile boolean disableRemoveWatches = false;

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
  }

  public void disableRemoveWatches() {
    this.disableRemoveWatches = true;
  }

  public static interface CollectionRemoved {
    void removed(String collection);
  }
  private volatile CollectionRemoved collectionRemoved;

  private static class CollectionWatch<T> {

    private final String collection;
    volatile AtomicInteger coreRefCount = new AtomicInteger();
    final Set<DocCollectionWatcher> stateWatchers = ConcurrentHashMap.newKeySet();

    final Set<CollectionPropsWatcher> propStateWatchers = ConcurrentHashMap.newKeySet();

    public CollectionWatch(String collection) {
      this.collection = collection;
    }

    public boolean canBeRemoved() {
      int refCount = coreRefCount.get();
      int watcherCount = stateWatchers.size();
      log.debug("{} watcher can be removed coreRefCount={}, stateWatchers={}", collection, refCount, watcherCount);
      return refCount <= 0 && watcherCount <= 0;
    }

  }


  public static final Set<String> KNOWN_CLUSTER_PROPS = Set.of(
      URL_SCHEME,
      CoreAdminParams.BACKUP_LOCATION,
      DEFAULT_SHARD_PREFERENCES,
      MAX_CORES_PER_NODE,
      SAMPLE_PERCENTAGE,
      SOLR_ENVIRONMENT,
      CollectionAdminParams.DEFAULTS);

  /**
   * Returns config set name for collection.
   * TODO move to DocCollection (state.json).
   *
   * @param collection to return config set name for
   */
  public String readConfigName(String collection) throws KeeperException {

    String configName = null;

    DocCollection docCollection = getCollectionOrNull(collection);
    if (docCollection != null) {
      configName = docCollection.getStr(CONFIGNAME_PROP);
      if (configName != null) {
        return configName;
      }
    }

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Loading collection config from: [{}]", path);

    try {

      byte[] data = zkClient.getData(path, null, null, true, true);
      if (data == null) {
        log.warn("No config data found at path {}.", path);
        throw new KeeperException.NoNodeException(path);
      }

      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.getStr(CONFIGNAME_PROP);

      if (configName == null) {
        log.warn("No config data found at path{}. ", path);
        throw new KeeperException.NoNodeException("No config data found at path: " + path);
      }
    } catch (InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      log.warn("Thread interrupted when loading config name for collection {}", collection);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Thread interrupted when loading config name for collection " + collection, e);
    }

    return configName;
  }


  private final SolrZkClient zkClient;

  protected final boolean closeClient;

  private volatile boolean closed = false;

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = zkClient;
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = false;
    this.securityNodeListener = securityNodeListener;
    assert ObjectReleaseTracker.track(this);
  }


  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    // MRM TODO: check this out
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
            // on reconnect, reload cloud info
            new OnReconnect() {
              @Override
              public void command() {
                ZkStateReader.this.createClusterStateWatchersAndUpdate();
              }

              @Override
              public String getName() {
                return "createClusterStateWatchersAndUpdate";
              }
            });

    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = true;
    this.securityNodeListener = null;
    try {
      zkClient.start();
    } catch (RuntimeException re) {
      log.error("Exception starting zkClient", re);
      zkClient.close(); // stuff has been opened inside the zkClient
      throw re;
    }
    assert ObjectReleaseTracker.track(this);
  }

  public ZkConfigManager getConfigManager() {
    return configManager;
  }

  // don't call this, used in one place

  public void forciblyRefreshAllClusterStateSlow() {
    // No need to set watchers because we should already have watchers registered for everything.
    try {
      refreshCollectionList();
      refreshLiveNodes();

      // Need a copy so we don't delete from what we're iterating over.
      watchedCollectionStates.forEach((name, coll) -> {
        DocCollection newState = null;

        if (!collectionStateLocks.containsKey(name)) {
          ReentrantLock collectionStateLock = new ReentrantLock(false);
          ReentrantLock oldLock = collectionStateLocks.putIfAbsent(name, collectionStateLock);
        }

        ReentrantLock collectionStateLock = collectionStateLocks.get(name);
        collectionStateLock.lock();
        try {
          try {
            newState = fetchCollectionState(name);
          } catch (Exception e) {
            log.error("problem fetching update collection state", e);
            return;
          }

          String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(name);
          try {
            newState = getAndProcessStateUpdates(name, stateUpdatesPath, newState, false);
          } catch (Exception e) {
            log.error("Error fetching state updates", e);
          }

          if (updateWatchedCollection(name, newState == null ? null : new ClusterState.CollectionRef(newState))) {
            constructState(newState, "forciblyRefreshAllClusterStateSlow");
          }
        } finally {
         collectionStateLock.unlock();
        }
      });

    } catch (Exception e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }

  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public void forciblyRefreshClusterStateSlow(String name) {
    try {
      refreshCollectionList();
      refreshLiveNodes();
      // Need a copy so we don't delete from what we're iterating over.

      Set<DocCollection> updatedCollections = new HashSet<>();

      if (!collectionStateLocks.containsKey(name)) {
        ReentrantLock collectionStateLock = new ReentrantLock(false);
        ReentrantLock oldLock = collectionStateLocks.putIfAbsent(name, collectionStateLock);
      }

      ReentrantLock collectionStateLock = collectionStateLocks.get(name);
      collectionStateLock.lock();
      try {

        DocCollection newState = fetchCollectionState(name);

        String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(name);
        try {
          newState = getAndProcessStateUpdates(name, stateUpdatesPath, newState, false);
        } catch (Exception e) {
          log.error("Error fetching state updates", e);
        }
//
//        if (updateWatchedCollection(name, newState == null ? null : new ClusterState.CollectionRef(newState))) {
//          updatedCollections.add(newState);
//         // constructState(newState, "forciblyRefreshClusterStateSlow");
//        }
      } finally {
        collectionStateLock.unlock();
      }

    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Refresh the set of live nodes.
   */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes();
  }

  public String compareStateVersions(String coll, int version, int updateHash) {
    log.debug("compareStateVersions {} {} {}", coll, version, updateHash);
    DocCollection collection = watchedCollectionStates.get(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() == version && updateHash == collection.getStateUpdates().hashCode()) {
      return null;
    }

    if (log.isDebugEnabled()) {
      log.debug("Wrong version from client [{}]!=[{}] updatesHash {}!={}", version, collection.getZNodeVersion(), updateHash, collection.getStateUpdates().hashCode());
    }

    // TODO: return state update hash as well and use this? Right now it's just a signal to get both on return
    return collection.getZNodeVersion() + ">" + collection.getStateUpdates().hashCode();
  }

  @SuppressWarnings({"unchecked"})
  public synchronized void createClusterStateWatchersAndUpdate() {
    if (isClosed()) {
      throw new AlreadyClosedException();
    }

    if (log.isDebugEnabled()) log.debug("createClusterStateWatchersAndUpdate");
    CountDownLatch latch = new CountDownLatch(1);

    Watcher watcher = event -> {
      if (closed || !EventType.NodeCreated.equals(event.getType())) {
        return;
      }

      latch.countDown();
    };

    try {
      Stat stat = zkClient.exists("/cluster_init_done", watcher);
      if (stat == null) {
        boolean success = latch.await(10000, TimeUnit.MILLISECONDS);
        if (!success) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "cluster not found/not ready");
        }
      } else {
        zkClient.removeWatches("/cluster_init_done",watcher, Watcher.WatcherType.Any, true, (rc, path, ctx) -> {}, "");
      }

    } catch (Exception e) {
      log.error("", e);
    }

    try {

      if (log.isDebugEnabled()) {
        log.debug("Updating cluster state from ZooKeeper... ");
      }

      // on reconnect of SolrZkClient force refresh and re-add watches.
      loadClusterProperties();

      this.liveNodesWatcher = new LiveNodeWatcher();
      this.liveNodesWatcher.createWatch();
      this.liveNodesWatcher.refresh();



      this.collectionsChildWatcher = new CollectionsChildWatcher();

      this.collectionsChildWatcher.createWatch();
      this.collectionsChildWatcher.refresh();



      refreshAliases(aliasesManager);

      if (securityNodeListener != null) {
        addSecurityNodeWatcher(pair -> {
          ConfigData cd = new ConfigData();
          cd.data = pair.first() == null || pair.first().length == 0 ? EMPTY_MAP : Utils.getDeepCopy((Map) fromJSON(pair.first()), 4, false);
          cd.version = pair.second() == null ? -1 : pair.second().getVersion();
          securityData = cd;
          securityNodeListener.run();
        });
        securityData = getSecurityProps(true);
      }

      collectionPropsObservers.forEach((k, v) -> {
        collectionPropsWatchers.computeIfAbsent(k, PropsWatcher::new).refreshAndWatch(true);
      });
    } catch (Exception e) {
      log.warn("", e);
      return;
    }
  }

  private void addSecurityNodeWatcher(final Callable<Pair<byte[], Stat>> callback)
      throws KeeperException, InterruptedException {
    zkClient.exists(SOLR_SECURITY_CONF_PATH,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events, and do not remove the watcher
            if (closed || EventType.None.equals(event.getType())) {
              return;
            }
            try {

              log.info("Updating [{}] ... ", SOLR_SECURITY_CONF_PATH);

              // remake watch
              final Stat stat = new Stat();
              byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
              if (EventType.NodeDeleted.equals(event.getType())) {
                // Node deleted, just recreate watch without attempting a read - SOLR-9679
                getZkClient().exists(SOLR_SECURITY_CONF_PATH, this);
              } else {
                data = getZkClient().getData(SOLR_SECURITY_CONF_PATH, this, stat);
              }
              try {
                callback.call(new Pair<>(data, stat));
              } catch (Exception e) {
                log.error("Error running collections node listener", e);
                return;
              }

            } catch (KeeperException e) {
              log.error("A ZK error has adding the security node watcher", e);
              return;
            } catch (InterruptedException e) {
              log.warn("", e);
              return;
            }
          }
        });
  }

  /**
   * Construct the total state view from all sources.
   *
   * @param collection collections that have changed since the last call,
   *                           and that should fire notifications
   */
  private void constructState(DocCollection collection, String caller) {
    if (this.closed) {
      log.debug("Already closed, won't notify");
      return;
    }
    log.debug("notify listeners on structure change {} {} {}", caller, collection, liveNodes);

    log.debug("clusterStateSet: interesting [{}] watched [{}] lazy [{}] total [{}]", collectionWatches.keySet().size(), watchedCollectionStates.size(), lazyCollectionStates.keySet().size(),
          allCollections.keySet().size());

    notifyCloudCollectionsListeners(true);

    if (collection != null) {
      notifyStateWatchers(collection.getName(), collection);
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionListLock = new Object();

  /**
   * Search for any lazy-loadable collections.
   */
  private void refreshCollectionList() throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zkClient.getChildren(COLLECTIONS_ZKNODE, null, null, true, true);
    } catch (KeeperException e) {
      log.warn("Error fetching collection names: [{}]", e.getMessage());
      return;
    }

    log.debug("found collections {}", children);
    // First, drop any children that disappeared.
    this.lazyCollectionStates.keySet().retainAll(children);
    watchedCollectionStates.keySet().retainAll(children);
    collectionStateLocks.keySet().retainAll(children);

    log.debug("lazyCollectionState retained collections {}", children);
    for (String coll : children) {
      ReentrantLock collectionStateLock = null;
      if (!collectionStateLocks.containsKey(coll)) {
        collectionStateLock = new ReentrantLock(false);
        ReentrantLock oldLock = collectionStateLocks.putIfAbsent(coll, collectionStateLock);
      }

      collectionWatches.compute(coll, (k, v) -> {
        if (v == null && !lazyCollectionStates.containsKey(coll)) {
          LazyCollectionRef docRef = new LazyCollectionRef(coll);
          lazyCollectionStates.put(coll, docRef);
          return null;
        }
        return v;
      });

      log.debug("Created lazy collection {} interesting [{}] watched [{}] lazy [{}] total [{}]", coll, collectionWatches.keySet().size(), watchedCollectionStates.size(),
          lazyCollectionStates.keySet().size(), allCollections.size());
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
 // private final Object refreshCollectionsSetLock = new Object();
  // Ensures that only the latest getChildren fetch gets applied.
  private final AtomicReference<Set<String>> lastFetchedCollectionSet = new AtomicReference<>();

  /**
   * Register a CloudCollectionsListener to be called when the set of collections within a cloud changes.
   */
  public void registerCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.add(cloudCollectionsListener);
    notifyNewCloudCollectionsListener(cloudCollectionsListener);
  }

  /**
   * Remove a registered CloudCollectionsListener.
   */
  public void removeCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.remove(cloudCollectionsListener);
  }

  private void notifyNewCloudCollectionsListener(CloudCollectionsListener listener) {
    notifications.submit(()-> listener.onChange(Collections.emptySet(), lastFetchedCollectionSet.get()));
  }

  private void notifyCloudCollectionsListeners() {
    notifyCloudCollectionsListeners(true);
  }

  private void notifyCloudCollectionsListeners(boolean notifyIfSame) {
    log.trace("Notify cloud collection listeners {}", notifyIfSame);
    Set<String> newCollections;
    Set<String> oldCollections;
    boolean fire = true;

    newCollections = getCurrentCollections();
//    oldCollections = lastFetchedCollectionSet.getAndSet(newCollections);
//    if (!newCollections.equals(oldCollections) || notifyIfSame) {
//      fire = true;
//    }

    log.trace("Should fire listeners? {} listeners={}", fire, cloudCollectionsListeners.size());
    if (fire) {
      cloudCollectionsListeners.forEach(new CloudCollectionsListenerConsumer(newCollections, newCollections));
    }
  }

  private Set<String> getCurrentCollections() {
    Set<String> collections = new HashSet<>(watchedCollectionStates.size() + lazyCollectionStates.size());
    collections.addAll(watchedCollectionStates.keySet());
    collections.addAll(lazyCollectionStates.keySet());
    return collections;
  }

  private class LazyCollectionRef extends ClusterState.CollectionRef {
    private final String collName;
    private long lastUpdateTime;
    private volatile DocCollection cachedDocCollection;

    public LazyCollectionRef(String collName, DocCollection cachedDocCollection) {
      super(null);
      this.collName = collName;
      this.lastUpdateTime = -1;
      this.cachedDocCollection = cachedDocCollection;
    }

    public LazyCollectionRef(String collName) {
      super(null);
      this.collName = collName;
      this.lastUpdateTime = -1;
    }

    @Override
    public DocCollection get(boolean allowCached) {
      gets.incrementAndGet();
      if (!allowCached || lastUpdateTime < 0 || System.nanoTime() - lastUpdateTime > LAZY_CACHE_TIME) {
        try {
          DocCollection cdc = getCollectionLive(collName);
         // log.debug("got live doc collection {}", cdc);
          if (cdc != null) {
            lastUpdateTime = System.nanoTime();
            cachedDocCollection = cdc;
            return cdc;
          }

        } catch (AlreadyClosedException e) {
          return cachedDocCollection;
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }

      return cachedDocCollection;
    }

    public DocCollection getCachedDocCollection() {
      return cachedDocCollection;
    }

    @Override
    public boolean isLazilyLoaded() {
      return true;
    }

    @Override
    public String toString() {
      return "LazyCollectionRef(" + collName + "::" + cachedDocCollection + ")";
    }
  }

  /**
   * Refresh live_nodes.
   */
  private void refreshLiveNodes() throws KeeperException, InterruptedException {

    SortedSet<String> oldLiveNodes;
    SortedSet<String> newLiveNodes = null;
    try {

      Stat stat = zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null, true);
      if (stat != null && this.liveNodesVersion >= 0) {
        if (stat.getCversion() < this.liveNodesVersion) {
          return;
        }
      }
      List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, null, stat, true);
      this.liveNodesVersion = stat.getCversion();
      newLiveNodes = new TreeSet<>(nodeList);
    } catch (KeeperException.NoNodeException e) {
      newLiveNodes = emptySortedSet();
    }

    oldLiveNodes = this.liveNodes;
    this.liveNodes = newLiveNodes;

    if (log.isInfoEnabled()) {
      log.info("Updated live nodes from ZooKeeper... ({}) -> ({})", oldLiveNodes.size(), newLiveNodes.size());
    }

    if (log.isTraceEnabled()) {
      log.trace("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
    }

    if (log.isDebugEnabled()) log.debug("Fire live node listeners");
    SortedSet<String> finalNewLiveNodes = newLiveNodes;

    liveNodesListeners.forEach(listener -> {
      notifications.submit(() -> {
        if (listener.onChange(new TreeSet<>(finalNewLiveNodes))) {
          removeLiveNodesListener(listener);
        }
      });
    });
  }

  public void registerClusterPropertiesListener(ClusterPropertiesListener listener) {
    // fire it once with current properties
    if (listener.onChange(getClusterProperties())) {
      removeClusterPropertiesListener(listener);
    } else {
      clusterPropertiesListeners.add(listener);
    }
  }

  public void removeClusterPropertiesListener(ClusterPropertiesListener listener) {
    clusterPropertiesListeners.remove(listener);
  }

  public void registerLiveNodesListener(LiveNodesListener listener) {
    // fire it once with current live nodes

    if (listener.onChange(new TreeSet<>(liveNodes))) {
      removeLiveNodesListener(listener);
    }

    liveNodesListeners.add(listener);
  }

  public void removeLiveNodesListener(LiveNodesListener listener) {
    liveNodesListeners.remove(listener);
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return new ClusterState(lazyCollectionStates, watchedCollectionStates);
  }

  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing ZkStateReader");
    assert closeTracker != null ? closeTracker.close() : true;

    closed = true;
    try {
      IOUtils.closeQuietly(clusterPropertiesWatcher);
      Future<?> cpc = collectionPropsCacheCleaner;
      if (cpc != null) {
        cpc.cancel(true);
      }
      watchedCollectionStates.clear();
      stateWatchersMap.forEach((s, stateWatcher) -> {
        IOUtils.closeQuietly(stateWatcher);
        if (!disableRemoveWatches) {
          stateWatcher.removeWatch();
        }
      });

      IOUtils.closeQuietly(this.liveNodesWatcher);
      IOUtils.closeQuietly(this.collectionsChildWatcher);
      if (closeClient) {
        IOUtils.closeQuietly(zkClient);
      }

      stateWatchersMap.clear();
    } finally {
      assert ObjectReleaseTracker.release(this);
    }
  }

  public String getLeaderUrl(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    Replica replica = getLeaderRetry(collection, shard, timeout);
    return replica.getCoreUrl();
  }

  public Replica getLeader(String collection, String shard) {
    try {
      return getLeaderRetry(collection, shard, 5000);
    } catch (InterruptedException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (TimeoutException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

//  public Replica getLeader(String collection, String shard) {
//    if (clusterState != null) {
//      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
//      Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
//      if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
//        return replica;
//      }
//    }
//    return null;
//  }

  public boolean isNodeLive(String node) {
    return getLiveNodes().contains(node);
  }

  public void setNode(String node) {
    this.node = node;
  }

  public void setLeaderChecker(IsLocalLeader isLocalLeader) {
    this.isLocalLeader = isLocalLeader;
  }

  public interface IsLocalLeader {
    boolean isLocalLeader(String name);
  }


  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, GET_LEADER_RETRY_DEFAULT_TIMEOUT, false);
  }

  public Replica getLeaderRetry(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, timeout, false);
  }

  public Replica getLeaderRetry(String collection, String shard, int timeout, boolean checkValidLeader) throws InterruptedException, TimeoutException {
    return getLeaderRetry(null, collection, shard, timeout, checkValidLeader);
  }
  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(Http2SolrClient httpClient, String collection, String shard, int timeout, boolean checkValidLeader) throws InterruptedException, TimeoutException {
    log.debug("get leader timeout={}", timeout);
    AtomicReference<Replica> returnLeader = new AtomicReference<>();
    DocCollection coll;

    TimeOut leaderVerifyTimeout = new TimeOut(timeout, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (true && !closed && !higherLevelClosed()) {

      try {
        waitForState(collection, timeout, TimeUnit.MILLISECONDS, (n, c) -> {
          if (c == null) return false;
          Slice slice = c.getSlice(shard);
          if (slice == null) return false;
          Replica leader = slice.getLeader();

          if (leader != null) {
            if (leader.getState() != Replica.State.ACTIVE) {
              return false;
            }
            log.debug("Found ACTIVE leader for slice={} leader={}", slice.getName(), leader);
            returnLeader.set(leader);
            return true;
          }

          return false;
        });
      } catch (TimeoutException e) {
        coll = watchedCollectionStates.get(collection);
        log.debug("timeout out while waiting to see leader in cluster state {} {}", shard, coll);
        throw new TimeoutException(
            "No registered leader was found after waiting for " + timeout + "ms " + ", collection: " + collection + " slice: " + shard + " saw state=" + coll
                + " with live_nodes=" + liveNodes);
      }

      Replica leader = returnLeader.get();
      if (checkValidLeader && leader != null) {
        log.debug("checking if found leader is valid {}", leader);
        if (node != null && isLocalLeader != null && leader.getNodeName().equals(node)) {
          if (isLocalLeader.isLocalLeader(leader.getName())) {
            break;
          }
        } else {

          try (Http2SolrClient client = new Http2SolrClient.Builder(leader.getBaseUrl()).idleTimeout(3000).connectionTimeout(2000).withHttpClient(httpClient).markInternalRequest().build()) {
            CoreAdminRequest.WaitForState prepCmd = new CoreAdminRequest.WaitForState();
            prepCmd.setCoreName(leader.getName());
            prepCmd.setLeaderName(leader.getName());
            prepCmd.setCollection(leader.getCollection());
            prepCmd.setShardId(leader.getSlice());
            prepCmd.setBasePath(leader.getBaseUrl());
            try {
              NamedList<Object> result = client.request(prepCmd);
              break;
            } catch (RejectedExecutionException | AlreadyClosedException e) {
              log.warn("Rejected or already closed, bailing {} {}", leader.getName(), e.getClass().getSimpleName());
              throw e;
            } catch (Exception e) {
              log.info("failed checking for leader {} {}", leader.getName(), e.getMessage());
              forciblyRefreshClusterStateSlow(collection);
              Thread.sleep(50);
            }
          }
        }
        if (leaderVerifyTimeout.hasTimedOut()) {
          log.debug("timeout out while checking if found leader is valid {}", leader);
          throw new TimeoutException("No registered leader was found " + "collection: " + collection + " slice: " + shard + " saw state=" + watchedCollectionStates.get(collection) +
              " with live_nodes=" + liveNodes);
        }

      } else {
        if (returnLeader.get() == null) {
          log.debug("return leader is null");
          throw new TimeoutException("No registered leader was found " + "collection: " + collection + " slice: " + shard + " saw state=" + watchedCollectionStates.get(collection) +
              " with live_nodes=" + liveNodes);
        }

        return leader;
      }
    }

    return returnLeader.get();
  }

  private boolean higherLevelClosed() {
    return zkClient.getHigherLevelIsClosed() != null && zkClient.getHigherLevelIsClosed().isClosed();
  }

  /**
   * Get path where shard leader properties live in zookeeper.
   */
  public static String getShardLeadersPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + "/" + collection + "/"
        + SHARD_LEADERS_ZKNODE + (shardId != null ? ("/" + shardId)
        : "") + "/leader";
  }

  /**
   * Get path where shard leader elections ephemeral nodes are.
   */
  public static String getShardLeadersElectPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + "/" + collection + "/"
        + LEADER_ELECT_ZKNODE + (shardId != null ? ("/" + shardId + "/" + ELECTION_NODE)
        : "");
  }


  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter, Replica.State mustMatchStateFilter2) {
    //TODO: We don't need all these getReplicaProps method overloading. Also, it's odd that the default is to return replicas of type TLOG and NRT only
    Set<Replica.State> matchFilters = new HashSet<>(2);
    matchFilters.add(mustMatchStateFilter);
    matchFilters.add(mustMatchStateFilter2);
    return getReplicaProps(collection, shardId, thisCoreNodeName, matchFilters, EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT));
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Collection<Replica.State> matchStateFilters, final EnumSet<Replica.Type> acceptReplicaType) {
    assert thisCoreNodeName != null;

    ClusterState.CollectionRef docCollectionRef = allCollections.get(collection);
    if (docCollectionRef == null) {
      return null;
    }
    final DocCollection docCollection = docCollectionRef.get();
    if (docCollection == null) return null;
    if (docCollection.getSlicesMap() == null) {
      return null;
    }
    Map<String, Slice> slices = docCollection.getSlicesMap();
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      return null;
    }

    Map<String, Replica> shardMap = replicas.getReplicasMap();
    List<Replica> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String, Replica> entry : shardMap.entrySet().stream().filter((e) -> acceptReplicaType.contains(e.getValue().getType())).collect(Collectors.toList())) {
      Replica nodeProps = entry.getValue();

      String coreNodeName = entry.getValue().getName();

      if (liveNodes.contains(nodeProps.getNodeName()) && !coreNodeName.equals(thisCoreNodeName)) {
        if (matchStateFilters == null || matchStateFilters.size() == 0 || matchStateFilters.contains(nodeProps.getState())) {
          nodes.add(nodeProps);
        }
      }
    }
    if (nodes.size() == 0) {
      // no replicas
      return null;
    }

    return nodes;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * Get a cluster property
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @param key          the property to read
   * @param defaultValue a default value to use if no such property exists
   * @param <T>          the type of the property
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Same as the above but allows a full json path as a list of parts
   *
   * @param keyPath      path to the property example ["collectionDefauls", "numShards"]
   * @param defaultValue a default value to use if no such property exists
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings({"unchecked"})
  public <T> T getClusterProperty(List<String> keyPath, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, keyPath);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Get all cluster properties for this cluster
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @return a Map of cluster properties
   */
  public Map<String, Object> getClusterProperties() {
    return Collections.unmodifiableMap(clusterProperties);
  }

  private final ClusterPropsWatcher clusterPropertiesWatcher = new ClusterPropsWatcher(ZkStateReader.CLUSTER_PROPS);

  @SuppressWarnings("unchecked")
  private void loadClusterProperties() {
    try {
        try {
          IOUtils.closeQuietly(clusterPropertiesWatcher);
          byte[] data = zkClient.getData(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, new Stat(), true);
          this.clusterProperties = ClusterProperties.convertCollectionDefaultsToNestedFormat((Map<String, Object>) Utils.fromJSON(data));
          log.debug("Loaded cluster properties: {}", this.clusterProperties);
          clusterPropertiesListeners.forEach((it) -> {
            notifications.submit(()-> it.onChange(getClusterProperties()));
          });
          return;
        } catch (KeeperException.NoNodeException e) {
          this.clusterProperties = Collections.emptyMap();
          if (log.isDebugEnabled()) {
            log.debug("Loaded empty cluster properties");
          }
        }
    } catch (KeeperException e) {
      log.error("Error reading cluster properties from zookeeper", SolrZkClient.checkInterrupted(e));
    } catch (InterruptedException e) {
      log.info("interrupted");
    }

  }

  /**
   * Get collection properties for a given collection. If the collection is watched, simply return it from the cache,
   * otherwise fetch it directly from zookeeper. This is a convenience for {@code getCollectionProperties(collection,0)}
   *
   * @param collection the collection for which properties are desired
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection) {
    return getCollectionProperties(collection, 0);
  }

  /**
   * Get and cache collection properties for a given collection. If the collection is watched, or still cached
   * simply return it from the cache, otherwise fetch it directly from zookeeper and retain the value for at
   * least cacheForMillis milliseconds. Cached properties are watched in zookeeper and updated automatically.
   * This version of {@code getCollectionProperties} should be used when properties need to be consulted
   * frequently in the absence of an active {@link CollectionPropsWatcher}.
   *
   * @param collection     The collection for which properties are desired
   * @param cacheForMillis The minimum number of milliseconds to maintain a cache for the specified collection's
   *                       properties. Setting a {@code CollectionPropsWatcher} will override this value and retain
   *                       the cache for the life of the watcher. A lack of changes in zookeeper may allow the
   *                       caching to remain for a greater duration up to the cycle time of {@link CacheCleaner}.
   *                       Passing zero for this value will explicitly remove the cached copy if and only if it is
   *                       due to expire and no watch exists. Any positive value will extend the expiration time
   *                       if required.
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection, long cacheForMillis) {
    PropsWatcher watcher = null;
    if (cacheForMillis > 0) {
      watcher = collectionPropsWatchers.compute(collection, (c, w) -> w == null ? new PropsWatcher(c, cacheForMillis) : w.renew(cacheForMillis));
    }
    VersionedCollectionProps vprops = watchedCollectionProps.get(collection);
    boolean haveUnexpiredProps = vprops != null && vprops.cacheUntilNs > System.nanoTime();
    long untilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(cacheForMillis, TimeUnit.MILLISECONDS);
    Map<String,String> properties;
    if (haveUnexpiredProps || (vprops != null && !zkClient.isConnected())) {
      properties = vprops.props;
      vprops.cacheUntilNs = Math.max(vprops.cacheUntilNs, untilNs);
    } else {
      try {
        VersionedCollectionProps vcp = fetchCollectionProperties(collection, watcher);
        properties = vcp.props;
        if (cacheForMillis > 0) {
          vcp.cacheUntilNs = untilNs;
          watchedCollectionProps.put(collection, vcp);
        } else {
          // we're synchronized on watchedCollectionProps and we can only get here if we have found an expired
          // vprops above, so it is safe to remove the cached value and let the GC free up some mem a bit sooner.
          if (!collectionPropsObservers.containsKey(collection)) {
            watchedCollectionProps.remove(collection);
          }
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading collection properties", SolrZkClient.checkInterrupted(e));
      }
    }
    return properties;
  }

  private static class VersionedCollectionProps {
    int zkVersion;
    Map<String, String> props;
    long cacheUntilNs = 0;

    VersionedCollectionProps(int zkVersion, Map<String, String> props) {
      this.zkVersion = zkVersion;
      this.props = props;
    }
  }

  public static String getCollectionPropsPath(final String collection) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/' + COLLECTION_PROPS_ZKNODE;
  }

  @SuppressWarnings("unchecked")
  private VersionedCollectionProps fetchCollectionProperties(String collection, PropsWatcher watcher) throws KeeperException, InterruptedException {
    final String znodePath = getCollectionPropsPath(collection);
    // lazy init cache cleaner once we know someone is using collection properties.
    if (collectionPropsCacheCleaner == null) {
      synchronized (this) { // There can be only one! :)
        if (collectionPropsCacheCleaner == null) {
          collectionPropsCacheCleaner = notifications.submit(new CacheCleaner());
        }
      }
    }

    try {
      IOUtils.closeQuietly(watcher);
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, watcher, stat, true, true);
      return new VersionedCollectionProps(stat.getVersion(), (Map<String,String>) Utils.fromJSON(data));
    } catch (ClassCastException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to parse collection properties for collection " + collection, e);
    } catch (KeeperException.NoNodeException e) {
      return new VersionedCollectionProps(-1, EMPTY_MAP);
    }
  }

  /**
   * Returns the content of /security.json from ZooKeeper as a Map
   * If the files doesn't exist, it returns null.
   */
  @SuppressWarnings({"unchecked"})
  public ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ConfigData(EMPTY_MAP, -1);
      return new ConfigData(securityData.data, securityData.version);
    }
    try {
      Stat stat = new Stat();
      if (getZkClient().exists(SOLR_SECURITY_CONF_PATH, true)) {
        final byte[] data = getZkClient().getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, stat, true);
        return data != null && data.length > 0 ?
            new ConfigData((Map<String, Object>) Utils.fromJSON(data), stat.getVersion()) :
            null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    }
    return null;
  }

  /**
   * Returns the baseURL corresponding to a given node's nodeName --
   * NOTE: does not (currently) imply that the nodeName (or resulting
   * baseURL) exists in the cluster.
   *
   * @lucene.experimental
   */
  @Override
  public String getBaseUrlForNodeName(final String nodeName) {
    return Utils.getBaseUrlForNodeName(nodeName, getClusterProperty(URL_SCHEME, "http"));
  }

  /**
   * Watches a single collection's format2 state.json.
   */
  class CollectionStateWatcher implements Watcher, Closeable, DoNotWrap {
    private final String coll;
    private final ReentrantLock collectionStateLock;
    private volatile StateUpdateWatcher stateUpdateWatcher;

    private volatile boolean running;

    private ReentrantLock ourLock = new ReentrantLock(false);

    private volatile boolean closed;

    private volatile boolean checkAgain = false;

    CollectionStateWatcher(String coll) {
      this.coll = coll;
      String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(coll);
      stateUpdateWatcher = new StateUpdateWatcher(stateUpdatesPath);
      ReentrantLock collStateLock = collectionStateLocks.get(coll);
      if (collStateLock == null) {
        collStateLock = new ReentrantLock(false);
        ReentrantLock oldLock = collectionStateLocks.putIfAbsent(coll, collStateLock);
        if (oldLock != null) {
          collStateLock = oldLock;
        }

      }

      this.collectionStateLock = collStateLock;
    }

    @Override
    public void process(WatchedEvent event) {

      // session events are not change events, and do not remove the watcher
      if (closed || !EventType.NodeDataChanged.equals(event.getType())) {
        return;
      }

      if (ZkStateReader.this.closed || closed || zkClient.isClosed()) {
        return;
      }

      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      Set<String> liveNodes = ZkStateReader.this.liveNodes;
      if (log.isDebugEnabled()) {
        log.debug("A cluster state change: [{}] for collection [{}] has occurred - updating... (live nodes size: [{}])", event, coll, liveNodes.size());
      }

      refresh();
    }

    /**
     * Refresh collection state from ZK and leave a watch for future changes.
     * As a side effect, updates {@link #watchedCollectionStates}
     * with the results of the refresh.
     */
    public void refresh() {

      ourLock.lock();
      try {
        if (running) {
          log.debug("already running a fetch, just say to do it again");
          checkAgain = true;
        } else {
          running = true;

          ParWork.getRootSharedExecutor().submit(() -> {
            log.debug("get collection lock for fetch");
            collectionStateLock.lock();
            try {
              do {
                log.debug("do the fetch");
                DocCollection newState = fetchCollectionState(coll);
                String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(coll);
                newState = getAndProcessStateUpdates(coll, stateUpdatesPath, newState, false);
                if (updateWatchedCollection(coll, newState == null ? null : new ClusterState.CollectionRef(newState))) {
                  constructState(newState, "state.json watcher");
                }
                ourLock.lock();
                try {
                  if (!checkAgain) {
                    running = false;
                    break;
                  }
                  checkAgain = false;
                } finally {
                  ourLock.unlock();
                }
              } while (true);
            } catch (Exception e) {
              log.error("A ZK error has occurred refreshing CollectionStateWatcher for collection={}", coll, e);
              throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred refreshing CollectionStateWatcher", e);
            } finally {
              collectionStateLock.unlock();
            }

          });
        }
      } finally {
        ourLock.unlock();
      }
    }

    public void createWatch() {
      String collectionCSNPath = getCollectionSCNPath(coll);
      try {
        zkClient.addWatch(collectionCSNPath, this, AddWatchMode.PERSISTENT, true, node == null);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      try {
        zkClient.addWatch(stateUpdateWatcher.stateUpdatesPath, stateUpdateWatcher, AddWatchMode.PERSISTENT, true, node == null);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    public void removeWatch() {

      String collectionCSNPath = getCollectionSCNPath(coll);
      try {
        zkClient.removeAllWatches(collectionCSNPath);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      try {
        zkClient.removeAllWatches(stateUpdateWatcher.stateUpdatesPath);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public void close() throws IOException {
      this.closed = true;
    }

    private class StateUpdateWatcher implements Watcher, Closeable, DoNotWrap {
      private final String stateUpdatesPath;
      private volatile boolean closed;

      private volatile boolean running;

      private ReentrantLock ourLock = new ReentrantLock(false);

      volatile boolean checkAgain = false;

      public StateUpdateWatcher(String stateUpdatesPath) {
        this.stateUpdatesPath = stateUpdatesPath;
      }

      @Override
      public void close() throws IOException {
        this.closed = true;
      }

      @Override
      public void process(WatchedEvent event) {
        if (closed || !EventType.NodeDataChanged.equals(event.getType())) {
          return;
        }

        if (ZkStateReader.this.closed || closed || zkClient.isClosed()) {
          return;
        }
        log.trace("_statupdates event {}", event);

        ourLock.lock();
        try {
          if (running) {
            checkAgain = true;
          } else {
            running = true;

            ParWork.getRootSharedExecutor().submit(() -> {

              collectionStateLock.lock();
              try {
                checkAgain = true;
                do {
                  checkAgain = false;

                  try {
                    getAndProcessStateUpdates(coll, stateUpdatesPath, watchedCollectionStates.get(coll), false);
                  } catch (AlreadyClosedException e) {

                  } catch (Exception e) {
                    log.error("Unwatched collection: [{}]", coll, e);
                  }
                  ourLock.lock();
                  try {
                    if (!checkAgain) {
                      running = false;
                      break;
                    }
                    checkAgain = false;
                  } finally {
                    ourLock.unlock();
                  }
                } while (true);

              } finally {
                collectionStateLock.unlock();
              }
            });
          }
        } finally {
          ourLock.unlock();
        }
      }
    }
  }

  /**
   * Watches collection properties
   */
  class PropsWatcher implements Watcher, Closeable {
    private final String coll;
    private long watchUntilNs;

    PropsWatcher(String coll) {
      this.coll = coll;
      watchUntilNs = 0;
    }

    PropsWatcher(String coll, long forMillis) {
      this.coll = coll;
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
    }

    public PropsWatcher renew(long forMillis) {
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
      return this;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || !EventType.NodeDataChanged.equals(event.getType())) {
        return;
      }

      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      boolean expired = System.nanoTime() > watchUntilNs;
      if (!collectionPropsObservers.containsKey(coll) && expired) {
        // No one can be notified of the change, we can ignore it and "unset" the watch
        log.debug("Ignoring property change for collection {}", coll);
        return;
      }

      log.info("A collection property change: [{}] for collection [{}] has occurred - updating...",
          event, coll);

      refreshAndWatch(true);
    }

    @Override
    public void close() throws IOException {
      String znodePath = getCollectionPropsPath(coll);

      try {
        zkClient.removeWatches(znodePath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }

    }

    /**
     * Refresh collection properties from ZK and leave a watch for future changes. Updates the properties in
     * watchedCollectionProps with the results of the refresh. Optionally notifies watchers
     */
    void refreshAndWatch(boolean notifyWatchers) {
      try {

        VersionedCollectionProps vcp = fetchCollectionProperties(coll, this);
        Map<String,String> properties = vcp.props;
        VersionedCollectionProps existingVcp = watchedCollectionProps.get(coll);
        if (existingVcp == null ||                   // never called before, record what we found
            vcp.zkVersion > existingVcp.zkVersion || // newer info we should update
            vcp.zkVersion == -1) {                   // node was deleted start over
          watchedCollectionProps.put(coll, vcp);
          if (notifyWatchers) {
            notifyPropsWatchers(coll, properties);
          }
          if (vcp.zkVersion == -1 && existingVcp != null) { // Collection DELETE detected

            // We should not be caching a collection that has been deleted.
            watchedCollectionProps.remove(coll);

            // core ref counting not relevant here, don't need canRemove(), we just sent
            // a notification of an empty set of properties, no reason to watch what doesn't exist.
            collectionPropsObservers.remove(coll);

            // This is the one time we know it's safe to throw this out. We just failed to set the watch
            // due to an NoNodeException, so it isn't held by ZK and can't re-set itself due to an update.
            collectionPropsWatchers.remove(coll);
          }
        }

      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("Lost collection property watcher for {} due to ZK error", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred refreshing property watcher", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Lost collection property watcher for {} due to the thread being interrupted", coll, e);
      }
    }
  }

  /**
   * Watches /collections children .
   */
  // MRM TODO: persistent watch
  class CollectionsChildWatcher implements Watcher, Closeable, DoNotWrap {

    protected final ReentrantLock ourLock = new ReentrantLock(false);

    private volatile boolean checkAgain = false;
    private volatile boolean running;

    @Override
    public void process(WatchedEvent event) {
      if (closed || !EventType.NodeChildrenChanged.equals(event.getType())) {
        return;
      }

      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      if (log.isDebugEnabled()) log.debug("A collections change: [{}], has occurred - updating...", event);

      ourLock.lock();
      try {
        if (running) {
          checkAgain = true;
        } else {
          running = true;
          ParWork.getRootSharedExecutor().submit(() -> {
            try {
              do {
                try {
                  refresh();
                } catch (Exception e) {
                  log.error("An error has occurred", e);
                  return;
                }

                ourLock.lock();
                try {
                  if (!checkAgain) {
                    running = false;
                    break;
                  }
                  checkAgain = false;

                } finally {
                  ourLock.unlock();
                }

              } while (true);

            } catch (Exception e) {
              log.error("exception submitting queue task", e);
            }

          });
        }
      } finally {
        ourLock.unlock();
      }
    }

    public void refresh() {
      try {
        refreshCollectionList();

        constructState(null, "collection child watcher");
      } catch (AlreadyClosedException e) {

      } catch (KeeperException e) {
        log.error("A ZK error has occurred refreshing CollectionsChildWatcher", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred refreshing CollectionsChildWatcher", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    public void createWatch() {
      try {
        zkClient.addWatch(COLLECTIONS_ZKNODE, this, AddWatchMode.PERSISTENT, true,node == null);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    public void removeWatch() {
      try {
        zkClient.removeAllWatches(COLLECTIONS_ZKNODE);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.warn("Exception removing watch", e);
      }
    }

    @Override
    public void close() throws IOException {
      removeWatch();
    }
  }

  /**
   * Watches the live_nodes and syncs changes.
   */
  class LiveNodeWatcher implements Watcher, Closeable {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || !EventType.NodeChildrenChanged.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      if (log.isDebugEnabled()) {
        log.debug("A live node change: [{}], has occurred - updating... (previous live nodes size: [{}])", event, liveNodes.size());
      }
      refresh();
    }

    public void refresh() {
      try {
        refreshLiveNodes();
      } catch (KeeperException.SessionExpiredException e) {
        // okay
      } catch (Exception e) {
        log.error("A ZK error has occurred refreshing CollectionsChildWatcher", e);
      }
    }

    public void createWatch() {
      try {
        zkClient.addWatch(LIVE_NODES_ZKNODE, this, AddWatchMode.PERSISTENT, true, node == null);
      } catch (Exception e) {
        log.warn("Exception creating watch", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Exception creating watch", e);
      }
    }

    public void removeWatch() {
      try {
        zkClient.removeAllWatches(LIVE_NODES_ZKNODE);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.warn("Exception removing watch", e);
      }
    }

    @Override
    public void close() throws IOException {
      removeWatch();
    }
  }

  public DocCollection getCollectionLive(String coll) {
    log.debug("getCollectionLive {}", coll);
    DocCollection newState;


    try {
      newState = fetchCollectionState(coll, true);
      String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(coll);
      newState = getAndProcessStateUpdates(coll, stateUpdatesPath, newState, true);
    } catch (KeeperException e) {
      log.warn("Zookeeper error getting latest collection state for collection={}", coll, e);
      return null;
    } catch (Exception e) {
      log.error("Exception getting fetching collection state: [{}]", coll, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception getting fetching collection state: " + coll, e);
    }
    return newState;
  }

  private DocCollection getAndProcessStateUpdates(String coll, String stateUpdatesPath, DocCollection docCollection, boolean live) throws KeeperException, InterruptedException {
    try {
      log.debug("get and process state updates for {} live={}", coll, live);
      if (docCollection == null) {
        return docCollection;
      }

      Stat stat;
      try {
        stat = getZkClient().exists(stateUpdatesPath, null, true, false);
        if (stat == null) {
          log.debug("stateUpdatesPath not found {}", stateUpdatesPath);
          return docCollection;
        }
      } catch (NoNodeException e) {
        log.info("No node found for {}", stateUpdatesPath);
        return docCollection;
      }

      Integer oldVersion = docCollection.getStateUpdatesZkVersion();
      if (oldVersion != null && stat.getVersion() < oldVersion) {
        if (log.isDebugEnabled()) log.debug("Will not apply state updates based on updates znode, they are for an older set of updates {}, ours is now {}", stat.getVersion(), oldVersion);
        return docCollection;
      }

      byte[] data;
      Stat stat2 = new Stat();
      try {
        data = getZkClient().getData(stateUpdatesPath, null, stat2, true, false);
      } catch (NoNodeException e) {
        log.info("No node found for {}", stateUpdatesPath);
        return docCollection;
      }

      if (data == null) {
        log.info("No data found for {}", stateUpdatesPath);
        //  docCollection.getStateUpdates().clear();
        return docCollection;
      }

      Map<String,Object> m = (Map) fromJSON(data);
      log.trace("Got additional state updates {}", m);

      stateUpdates.compute(coll, (k, v) -> {
        if (v == null) {
          return m;
        }

        Integer zkVersion = (Integer) v.get("_ver_");
        if (zkVersion == null) {
          zkVersion = -1;
        }
        if (stat2.getVersion() > zkVersion) {
          return m;
        }
        return v;
      });

      if (m.size() == 0) {
        log.debug("No updates found at {} {} {} {} exists_ver={}", stateUpdatesPath, data.length, new String(data), stat2.getVersion(), stat.getVersion());
        return docCollection;
      }

      Object csVer = m.get("_cs_ver_");
      int version;
      if (csVer == null) {
        version = -1;
      } else {
        version = Integer.parseInt((String) csVer);
      }

      log.info("Got additional state updates with znode version {} for cs version {} updates={}", stat2.getVersion(), version, m);

      //m.remove("_cs_ver_");
      m.put("_ver_", stat2.getVersion());

      Set<Entry<String,Object>> entrySet = m.entrySet();

      if (docCollection != null) {
        if (stat2.getVersion() < docCollection.getStateUpdatesZkVersion()) {
          log.info("Will not apply state updates based on state.json znode, they are for an older state.json {}, ours is now {}", stat2.getVersion(), docCollection.getStateUpdatesZkVersion());
          return docCollection;
        }

        //        Integer oldVersion = docCollection.getStateUpdatesVersion();
        //        if (oldVersion != null && stat2.getVersion() < oldVersion) {
        //          if (log.isDebugEnabled()) log.debug("Will not apply state updates, they are for an older set of updates {}, ours is now {}", stat2.getVersion(), oldVersion);
        //          return docCollection;
        //        }

        for (Entry<String,Object> entry : entrySet) {
          String id = entry.getKey();
          if (id.equals("_ver_") || id.equals("_cs_ver_")) continue;
          Replica.State state = null;
          if (!entry.getValue().equals("l")) {
            state = Replica.State.shortStateToState((String) entry.getValue());
          }

          Replica replica = docCollection.getReplicaById(docCollection.getId() + "-" + id);
          log.trace("Got additional state update {} replica={} id={} ids={} {}", state == null ? "leader" : state, replica == null ? null : replica.getName(), id, docCollection.getReplicaByIds());

          if (replica != null) {

            //     if (replica.getState() != state || entry.getValue().equals("l")) {
            Slice slice = docCollection.getSlice(replica.getSlice());
            Map<String,Replica> replicasMap = slice.getReplicasCopy();
            Map properties = new HashMap(replica.getProperties());

            if (entry.getValue().equals("l")) {
              log.trace("state is leader, set to active and leader prop id={}", replica.getId());
              properties.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE);
              properties.put(LEADER_PROP, "true");

              for (Replica r : replicasMap.values()) {
                if (replica.getName().equals(r.getName())) {
                  continue;
                }
                log.trace("process non leader {} {}", r, r.getProperty(LEADER_PROP));
                if ("true".equals(r.getProperties().get(LEADER_PROP))) {
                  log.debug("remove leader prop {}", r);
                  Map<String,Object> props = new HashMap<>(r.getProperties());
                  props.remove(LEADER_PROP);
                  Replica newReplica = new Replica(r.getName(), props, coll, docCollection.getId(), r.getSlice(), r.getBaseUrl());
                  replicasMap.put(r.getName(), newReplica);
                }
              }
            } else if (state != null) {
              log.trace("std state, set to {}", state);
              properties.put(ZkStateReader.STATE_PROP, state.toString());
              if ("true".equals(properties.get(LEADER_PROP))) {
                properties.remove(LEADER_PROP);
              }
            }

            Replica newReplica = new Replica(replica.getName(), properties, coll, docCollection.getId(), replica.getSlice(), replica.getBaseUrl());

            log.trace("add new replica {}", newReplica);

            replicasMap.put(replica.getName(), newReplica);

            Slice newSlice = new Slice(slice.getName(), replicasMap, slice.getProperties(), coll, docCollection.getId());

            Map<String,Slice> newSlices = docCollection.getSlicesCopy();

            newSlices.put(slice.getName(), newSlice);

            log.trace("add new slice leader={} {}", newSlice.getLeader(), newSlice);

            DocCollection newDocCollection = new DocCollection(coll, newSlices, docCollection.getProperties(), docCollection.getRouter(), docCollection.getZNodeVersion(), m);
            docCollection = newDocCollection;

          } else {
            if (log.isDebugEnabled()) log.debug("Could not find core to update local state {} {}", id, state);
          }
        }

        if (!live) log.debug("Set a new clusterstate based on update diff {} live={}", docCollection, live);

        if (!live) {
          if (updateWatchedCollection(coll, new ClusterState.CollectionRef(docCollection))) {
            constructState(docCollection, "state.json state updates watcher");
          }
        }

      }

    } catch (Exception e) {
      log.error("Exception trying to process additional updates", e);
    }
    return docCollection;
  }

  private DocCollection fetchCollectionState(String coll) throws KeeperException, InterruptedException {
    return fetchCollectionState(coll, false);
  }

  private DocCollection fetchCollectionState(String coll, boolean live) throws KeeperException, InterruptedException {
    DocCollection docCollection = null;
    String collectionPath = getCollectionPath(coll);
    if (log.isDebugEnabled()) log.debug("Looking at fetching full clusterstate collection={}", coll);

    int version = 0;


    if (!live) {
      // version we would get
      docCollection = watchedCollectionStates.get(coll);

      if (docCollection != null) {
        Stat stateStat = zkClient.exists(collectionPath, null, true, false);
        if (stateStat != null) {
          version = stateStat.getVersion();

          if (log.isDebugEnabled()) log.debug("version for cs is {}, local version is {}", version, docCollection.getZNodeVersion());

          int localVersion = docCollection.getZNodeVersion();
          if (log.isDebugEnabled()) log.debug("found version {}, our local version is {}, has updates {}", version, localVersion, docCollection.hasStateUpdates());

          if (localVersion >= version) {
            return docCollection;
          }

        } else {
          return null;
        }
      }
    }
    log.debug("getting latest state.json for {}", coll);
    Stat stat = new Stat();
    byte[] data;
    try {
      data = zkClient.getData(collectionPath, null, stat, true, true);
    } catch (NoNodeException e) {
      log.debug("no state.json znode found");
      return null;
    }

    if (data == null) {
      log.debug("no data found at state.json node");
      return null;
    }

    log.debug("returning state.json with version {}", stat.getVersion());
    docCollection = ClusterState.createDocCollectionFromJson(this, stat.getVersion(), data);

    return docCollection;
  }

  public static String getCollectionPathRoot(String coll) {
    return COLLECTIONS_ZKNODE + "/" + coll;
  }

  public static String getCollectionPath(String coll) {
    return getCollectionPathRoot(coll) + "/state.json";
  }

  public static String getCollectionSCNPath(String coll) {
    return getCollectionPathRoot(coll) + "/" + STRUCTURE_CHANGE_NOTIFIER;
  }

  public static String getCollectionStateUpdatesPath(String coll) {
    return getCollectionPathRoot(coll) + "/" + STATE_UPDATES;
  }
  /**
   * Notify this reader that a local Core is a member of a collection, and so that collection
   * state should be watched.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * The number of cores per-collection is tracked, and adding multiple cores from the same
   * collection does not increase the number of watches.
   *
   * @param collection the collection that the core is a member of
   * @see ZkStateReader#unregisterCore(String, String)
   */
  public void registerCore(String collection, String coreName) {

    if (log.isDebugEnabled()) log.debug("register core for collection {} {}", collection, coreName);
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    if (!registeredCores.add(coreName)) {
      return;
    }

    AtomicReference<CollectionStateWatcher> createSw = new AtomicReference();

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>(collection);
        CollectionStateWatcher sw = new CollectionStateWatcher(collection);
        stateWatchersMap.put(collection, sw);
        createSw.set(sw);
        sw.createWatch();
      }

      v.coreRefCount.incrementAndGet();
      return v;
    });
    CollectionStateWatcher sw = createSw.get();
    if (sw != null) {
      sw.refresh();
    }

  }

  public boolean watched(String collection) {
    return collectionWatches.containsKey(collection);
  }

  /**
   * Notify this reader that a local core that is a member of a collection has been closed.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * If no cores are registered for a collection, and there are no {@link org.apache.solr.common.cloud.CollectionStateWatcher}s
   * for that collection either, the collection watch will be removed.
   *
   * @param collection the collection that the core belongs to
   * @param coreName
   */
  public void unregisterCore(String collection, String coreName) {
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    if (registeredCores.remove(coreName)) {
      return;
    }

    AtomicReference<CollectionStateWatcher> reconstructState = new AtomicReference();

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) return null;
      if (v.coreRefCount.get() > 0)
        v.coreRefCount.decrementAndGet();
      if (v.canBeRemoved()) {
        log.debug("no longer watch collection {}", collection);


        DocCollection state = watchedCollectionStates.remove(collection);
        LazyCollectionRef docRef = new LazyCollectionRef(collection, state);
        lazyCollectionStates.put(collection, docRef);
        CollectionStateWatcher stateWatcher = stateWatchersMap.remove(collection);
        if (stateWatcher != null) {
          IOUtils.closeQuietly(stateWatcher);
          stateWatcher.removeWatch();
          reconstructState.set(stateWatcher);
        }
        return null;
      }
      return v;
    });

    CollectionStateWatcher sw = reconstructState.get();
    if (sw != null) {

      //constructState(collection, "unregisterCore");
    }
  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   * <em>or</em> the set of live nodes changes.
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   *
   * <p>
   * This is method is just syntactic sugar for registering both a {@link DocCollectionWatcher} and
   * a {@link LiveNodesListener}.  Callers that only care about one or the other (but not both) are
   * encouraged to use the more specific methods register methods as it may reduce the number of
   * ZooKeeper watchers needed, and reduce the amount of network/cpu used.
   * </p>
   *
   * @see #registerDocCollectionWatcher
   * @see #registerLiveNodesListener
   */
  public void registerCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher stateWatcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper = new DocCollectionAndLiveNodesWatcherWrapper(collection, stateWatcher);

    registerDocCollectionWatcher(collection, wrapper);
    registerLiveNodesListener(wrapper);

    if (stateWatcher.onStateChanged(liveNodes, watchedCollectionStates.get(collection)) == true) {
      removeCollectionStateWatcher(collection, stateWatcher);
    }
  }

  /**
   * Register a DocCollectionWatcher to be called when the state of a collection changes
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   */
  public void registerDocCollectionWatcher(String collection, DocCollectionWatcher docCollectionWatcher) {
    log.debug("registerDocCollectionWatcher {}", collection);

    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }
    AtomicReference<CollectionStateWatcher> watchSet = new AtomicReference<>();
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        log.debug("creating CollectionStateWatcher for {} and refreshing", collection);
        v = new CollectionWatch<>(collection);
        CollectionStateWatcher sw = new CollectionStateWatcher(collection);
        stateWatchersMap.put(collection, sw);
        sw.createWatch();
        watchSet.set(sw);
      }
      v.stateWatchers.add(docCollectionWatcher);
      log.debug("Adding a DocCollectionWatcher for collection={} currentCount={}", collection, v.stateWatchers.size());
      return v;
    });

    CollectionStateWatcher sw = watchSet.get();
    if (sw != null) {
      sw.refresh();
    }

    if (docCollectionWatcher.onStateChanged(watchedCollectionStates.get(collection))) {
      removeDocCollectionWatcher(collection, docCollectionWatcher);
    }
  }

  public DocCollection getCollection(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection : " + collection + " collections=" + allCollections.keySet());
    return coll;
  }

  public DocCollection getCollectionOrNull(String collection) {
    ClusterState.CollectionRef coll = allCollections.get(collection);
    if (coll == null) return null;
    return coll.get();
  }

  public ClusterState.CollectionRef getCollectionRef(String collection) {
    return allCollections.get(collection);
  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * <p>
   * This implementation utilizes {@link org.apache.solr.common.cloud.CollectionStateWatcher} internally.
   * Callers that don't care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate}
   * instead
   * </p>
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   * @see #registerCollectionStateWatcher
   */
  public void waitForState(final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {
    log.debug("wait for state {}", collection);
    TimeOut timeout = new TimeOut(wait, unit, TimeSource.NANO_TIME);

    if (predicate.matches(getLiveNodes(), watchedCollectionStates.get(collection))) {
      return;
    }

    final CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<DocCollection> docCollection = new AtomicReference<>();
    org.apache.solr.common.cloud.CollectionStateWatcher watcher = new PredicateMatcher(predicate, latch, docCollection).invoke();
    registerCollectionStateWatcher(collection, watcher);
    try {
      // wait for the watcher predicate to return true, or time out
      while (!latch.await(2500, TimeUnit.MILLISECONDS)) {
        if (timeout.hasTimedOut()) {
          throw new TimeoutException("Timeout waiting to see state for collection=" + collection + " :" + "live=" + liveNodes + docCollection.get());
        }
        if (closed || (getZkClient().getHigherLevelIsClosed() != null && getZkClient().getHigherLevelIsClosed().isClosed())) {
          throw new AlreadyClosedException();
        }
      }
    } finally {
      removeCollectionStateWatcher(collection, watcher);
    }
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas) throws TimeoutException {
    waitForActiveCollection(collection, wait, unit, shards, totalReplicas, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas, boolean exact) throws TimeoutException {
    waitForActiveCollection(collection, wait, unit, false, shards, totalReplicas, exact, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, boolean justLeaders,  int shards, int totalReplicas, boolean exact, boolean checkValidLeaders)
      throws TimeoutException {
    waitForActiveCollection(null, collection, wait, unit, justLeaders, shards, totalReplicas, exact, checkValidLeaders);
  }

  public void waitForActiveCollection(Http2SolrClient client, String collection, long wait, TimeUnit unit, boolean justLeaders,  int shards, int totalReplicas, boolean exact, boolean checkValidLeaders)
      throws TimeoutException {
    log.debug("waitForActiveCollection: {} interesting [{}] watched [{}] lazy [{}] total [{}]", collection, collectionWatches.keySet().size(), watchedCollectionStates.size(), lazyCollectionStates.keySet().size(),
        allCollections.size());

    assert collection != null;
    TimeOut leaderVerifyTimeout = new TimeOut(wait, unit, TimeSource.NANO_TIME);
    while (true && !closed) {
      Set<Replica> returnLeaders = ConcurrentHashMap.newKeySet();
      CollectionStatePredicate predicate = expectedShardsAndActiveReplicas(justLeaders, shards, totalReplicas, exact, returnLeaders);

      AtomicReference<DocCollection> state = new AtomicReference<>();
      AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
      try {
        waitForState(collection, wait, unit, (n, c) -> {
          state.set(c);
          liveNodesLastSeen.set(n);

          return predicate.matches(n, c);
        });
      } catch (TimeoutException e) {
        throw new TimeoutException("Failed while waiting for active collection" + "\n" + e.getMessage() + " \nShards:" + shards + " Replicas:" + totalReplicas + "\nLive Nodes: " + Arrays
            .toString(liveNodesLastSeen.get().toArray()) + "\nLast available state: " + state.get());
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new RuntimeException("", e);
      }

      if (checkValidLeaders) {
        Boolean success;

        try (Http2SolrClient httpClient = new Http2SolrClient.Builder("").idleTimeout(3000).connectionTimeout(2000).withHttpClient(client).markInternalRequest().build()) {
          success = checkLeaders(collection, returnLeaders, httpClient);
        }

        if (success == null || !success) {
          log.info("Failed confirming all shards have valid leaders");
        } else {
          log.info("done checking valid leaders on active collection success={}", success);
          break;
        }
        if (leaderVerifyTimeout.hasTimedOut()) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "No registered leader was found " + "collection: " + collection + " saw state=" + allCollections.get(collection) + " with live_nodes=" + liveNodes);
        }

      } else {
        break;
      }
    }

  }

  private Boolean checkLeaders(String collection, Set<Replica> shards, Http2SolrClient client) {

    boolean success = true;

    for (Replica leader : shards) {
      if (node != null && isLocalLeader != null && leader.getNodeName().equals(node)) {
        if (!isLocalLeader.isLocalLeader(leader.getName())) {
          log.info("failed checking for local leader {} {}", leader.getName());
          success = false;
          try {
            Thread.sleep(50);
          } catch (InterruptedException interruptedException) {
            ParWork.propagateInterrupt(interruptedException);
          }
          break;
        }
      } else {
        CoreAdminRequest.WaitForState prepCmd = new CoreAdminRequest.WaitForState();
        prepCmd.setCoreName(leader.getName());
        prepCmd.setLeaderName(leader.getName());
        prepCmd.setCollection(leader.getCollection());
        prepCmd.setShardId(leader.getSlice());

        prepCmd.setBasePath(leader.getBaseUrl());

        try {
          NamedList<Object> result = client.request(prepCmd);
          log.info("Leader looks valid {}", leader);

        } catch (RejectedExecutionException | AlreadyClosedException e) {
          log.warn("Rejected or already closed, bailing {} {}", leader.getName(), e.getClass().getSimpleName());
          throw e;
        } catch (Exception e) {
          log.info("failed checking for leader {} {}", leader.getName(), e.getMessage());
          success = false;
          try {
            Thread.sleep(50);
          } catch (InterruptedException interruptedException) {
            ParWork.propagateInterrupt(interruptedException);
          }
          break;
        }
      }
    }

    return success;
  }

  /**
   * Block until a LiveNodesStatePredicate returns true, or the wait times out
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * @param wait      how long to wait
   * @param unit      the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   */
  public void waitForLiveNodes(long wait, TimeUnit unit, LiveNodesPredicate predicate)
      throws InterruptedException, TimeoutException {

    if (predicate.matches(liveNodes)) {
      return;
    }

    final CountDownLatch latch = new CountDownLatch(1);

    LiveNodesListener listener = (n) -> {
      boolean matches = predicate.matches(n);
      if (matches)
        latch.countDown();
      return matches;
    };

    registerLiveNodesListener(listener);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException("Timeout waiting for live nodes, currently they are: " + liveNodes);

    } finally {
      removeLiveNodesListener(listener);
    }
  }


  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerCollectionStateWatcher
   */
  public void removeCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher watcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper
        = new DocCollectionAndLiveNodesWatcherWrapper(collection, watcher);

    removeLiveNodesListener(wrapper);
    removeDocCollectionWatcher(collection, wrapper);
  }

  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerDocCollectionWatcher
   */
  public void removeDocCollectionWatcher(String collection, DocCollectionWatcher watcher) {

    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    AtomicReference<CollectionStateWatcher> swRef = new AtomicReference();
    AtomicReference<ClusterState.CollectionRef> ref = new AtomicReference();

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) return null;
      v.stateWatchers.remove(watcher);
      log.debug("remove watcher for collection {} currentCount={}", collection, v.stateWatchers.size());
      if (v.canBeRemoved()) {
        log.debug("no longer watch collection {}", collection);
        CollectionStateWatcher stateWatcher = stateWatchersMap.remove(collection);
        if (stateWatcher != null) {
          IOUtils.closeQuietly(stateWatcher);
          stateWatcher.removeWatch();
          swRef.set(stateWatcher);
        }
        DocCollection docCollection = watchedCollectionStates.remove(collection);
        LazyCollectionRef docRef = new LazyCollectionRef(collection, docCollection);
        lazyCollectionStates.put(collection, docRef);
        
        ref.set(docRef);
        return null;
      }
      return v;
    });

  }

  /* package-private for testing */
  Set<DocCollectionWatcher> getStateWatchers(String collection) {
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }
    final Set<DocCollectionWatcher> watchers = new HashSet<>();

    collectionWatches.compute(collection, (k, v) -> {
      if (v != null) {
        watchers.addAll(v.stateWatchers);
      }
      return v;
    });

    return watchers;
  }

  // returns true if the state has changed
  private boolean updateWatchedCollection(String coll, ClusterState.CollectionRef newState) {
   // log.debug("updateWatchedCollection for [{}] [{}]", coll, newState);
    try {
      if (newState == null) {
        if (log.isDebugEnabled()) log.debug("Removing cached collection state for [{}]", coll);

        return true;
      }

      AtomicBoolean update = new AtomicBoolean(false);
      DocCollection newDocState = newState.get(false);
      if (newDocState == null) {
        update.set(false);
        return false;
      }

      watchedCollectionStates.compute(coll, (k, v) -> {
        DocCollection docColl = v;

        if (docColl == null) {
          update.set(true);
          lazyCollectionStates.remove(coll);
          return newDocState;
        }

        if ((newDocState.getZNodeVersion() > docColl.getZNodeVersion()) || (newDocState.getZNodeVersion() == docColl.getZNodeVersion() &&
            newDocState.getStateUpdatesZkVersion() > docColl.getStateUpdatesZkVersion())) {

          log.debug("new state does have updates, replace >= {} {} {} {}", newDocState.getZNodeVersion(), docColl.getZNodeVersion(),
              newDocState.getStateUpdatesZkVersion(), docColl.getStateUpdatesZkVersion());
          lazyCollectionStates.remove(coll);
          update.set(true);
          return newDocState;
        }

        log.debug("don't replace state {}!={} or {}!={}", docColl.getZNodeVersion(), newDocState.getZNodeVersion(), docColl.getStateUpdates().hashCode(), newDocState.getStateUpdates().hashCode());
        update.set(false);
        return v;
      });

      return update.get();
    } catch (Exception e) {
      log.error("Failing updating clusterstate", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void registerCollectionPropsWatcher(final String collection, CollectionPropsWatcher propsWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionPropsObservers.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>(collection);
        watchSet.set(true);
      }
      v.propStateWatchers.add(propsWatcher);
      return v;
    });

    if (watchSet.get()) {
      collectionPropsWatchers.computeIfAbsent(collection, PropsWatcher::new).refreshAndWatch(false);
    }
  }

  public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
    collectionPropsObservers.compute(collection, (k, v) -> {
      if (v == null)
        return null;
      v.propStateWatchers.remove(watcher);
      if (v.canBeRemoved()) {
        // don't want this to happen in middle of other blocks that might add it back.
        watchedCollectionProps.remove(collection);

        return null;
      }
      return v;
    });
  }

  public void setCollectionRemovedListener(CollectionRemoved listener) {
    this.collectionRemoved = listener;
  }

  public static class ConfigData {
    public Map<String, Object> data;
    public int version;

    public ConfigData() {
    }

    public ConfigData(Map<String, Object> data, int version) {
      this.data = data;
      this.version = version;

    }
  }

  private void notifyStateWatchers(String collection, DocCollection collectionState) {
    if (this.closed) {
      log.debug("Already closed, won't notify");
      return;
    }
    log.trace("Notify state watchers [{}] {}", collectionWatches.keySet(), collectionState.getName());

    try {
      notifications.submit(new Notification(collection, collectionState, collectionWatches));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection notifications for {}", collection, e);
      }
    }

  }

  private class Notification implements Runnable {

    final String collection;
    final DocCollection collectionState;

    private final ConcurrentHashMap<String,CollectionWatch<DocCollectionWatcher>> collectionWatches;

    public Notification(String collection, DocCollection collectionState, ConcurrentHashMap<String, CollectionWatch<DocCollectionWatcher>> collectionWatches) {
      this.collection = collection;
      this.collectionState = collectionState;
      this.collectionWatches = collectionWatches;
    }

    @Override
    public void run() {
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      log.trace("notify on state change {}", collectionWatches.keySet());
      List<DocCollectionWatcher> watchers;
      CollectionWatch<DocCollectionWatcher> watches = collectionWatches.get(collection);
      if (watches != null) {
        watchers = new ArrayList<>(watches.stateWatchers.size());
      } else {
        watchers = new ArrayList<>();
      }

      collectionWatches.compute(collection, (k, v) -> {
        if (v == null) return null;
        watchers.addAll(v.stateWatchers);
        return v;
      });

      watchers.forEach(watcher -> {
        notifications.submit(() -> {
          log.trace("Notify DocCollectionWatcher {} {}", watcher, collectionState);
          try {
            if (watcher.onStateChanged(collectionState)) {
              removeDocCollectionWatcher(collection, watcher);
            }
          } catch (Exception exception) {
            ParWork.propagateInterrupt(exception);
            log.warn("Error on calling watcher", exception);
          }
        });
      });

    }
  }

  //
  //  Aliases related
  //

  /**
   * Access to the {@link Aliases}.
   */
  public final AliasesManager aliasesManager = new AliasesManager();

  /**
   * Get an immutable copy of the present state of the aliases. References to this object should not be retained
   * in any context where it will be important to know if aliases have changed.
   *
   * @return The current aliases, Aliases.EMPTY if not solr cloud, or no aliases have existed yet. Never returns null.
   */
  public Aliases getAliases() {
    return aliasesManager.getAliases();
  }

  // called by createClusterStateWatchersAndUpdate()
  private void refreshAliases(AliasesManager watcher) throws KeeperException, InterruptedException {
    constructState(null, "refreshAliases");
    zkClient.exists(ALIASES, watcher);
    aliasesManager.update();
  }

  /**
   * A class to manage the aliases instance, including watching for changes.
   * There should only ever be one instance of this class
   * per instance of ZkStateReader. Normally it will not be useful to create a new instance since
   * this watcher automatically re-registers itself every time it is updated.
   */
  public class AliasesManager implements Watcher { // the holder is a Zk watcher
    // note: as of this writing, this class if very generic. Is it useful to use for other ZK managed things?
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private volatile Aliases aliases = Aliases.EMPTY;

    public Aliases getAliases() {
      return aliases; // volatile read
    }

    /**
     * Writes an updated {@link Aliases} to zk.
     * It will retry if there are races with other modifications, giving up after 30 seconds with a SolrException.
     * The caller should understand it's possible the aliases has further changed if it examines it.
     */
    public void applyModificationAndExportToZk(UnaryOperator<Aliases> op) {
      // The current aliases hasn't been update()'ed yet -- which is impossible?  Any way just update it first.
//      if (aliases.getZNodeVersion() == -1) {
//        try {
//          boolean updated = update();
//          assert updated;
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
//        } catch (KeeperException e) {
//          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
//        }
//      }
//
//      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
//      // note: triesLeft tuning is based on ConcurrentCreateRoutedAliasTest
//      for (int triesLeft = 5; triesLeft > 0; triesLeft--) {
//        // we could synchronize on "this" but there doesn't seem to be a point; we have a retry loop.
//        Aliases curAliases = getAliases();
//        Aliases modAliases = op.apply(curAliases);
//        final byte[] modAliasesJson = modAliases.toJSON();
//        if (curAliases == modAliases) {
//          log.debug("Current aliases has the desired modification; no further ZK interaction needed.");
//          return;
//        }
//
//        try {
//          try {
//            final Stat stat = getZkClient().setData(ALIASES, modAliasesJson, curAliases.getZNodeVersion(), true);
//            setIfNewer(Aliases.fromJSON(modAliasesJson, stat.getVersion()));
//            return;
//          } catch (KeeperException.BadVersionException e) {
//            log.debug("{}", e, e);
//            log.warn("Couldn't save aliases due to race with another modification; will update and retry until timeout");
//            Thread.sleep(250);
//            // considered a backoff here, but we really do want to compete strongly since the normal case is
//            // that we will do one update and succeed. This is left as a hot loop for limited tries intentionally.
//            // More failures than that here probably indicate a bug or a very strange high write frequency usage for
//            // aliases.json, timeouts mean zk is being very slow to respond, or this node is being crushed
//            // by other processing and just can't find any cpu cycles at all.
//            update();
//            if (deadlineNanos < System.nanoTime()) {
//              throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out trying to update aliases! " +
//                  "Either zookeeper or this node may be overloaded.");
//            }
//          }
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
//        } catch (KeeperException e) {
//          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
//        }
//      }
//      throw new SolrException(ErrorCode.SERVER_ERROR, "Too many successive version failures trying to update aliases");
    }

    /**
     * Ensures the internal aliases is up to date. If there is a change, return true.
     *
     * @return true if an update was performed
     */
    public boolean update() throws KeeperException, InterruptedException {
      log.debug("Checking ZK for most up to date Aliases {}", ALIASES);
      // Call sync() first to ensure the subsequent read (getData) is up to date.
      // MRM TODO: review
      zkClient.getConnectionManager().getKeeper().sync(ALIASES, null, null);
      Stat stat = new Stat();
      final byte[] data = zkClient.getData(ALIASES, null, stat, true);
      return setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
    }

    // ZK Watcher interface
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || EventType.None.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      try {
        log.debug("Aliases: updating");

        // re-register the watch
        Stat stat = new Stat();
        final byte[] data = zkClient.getData(ALIASES, this, stat, true);
        // note: it'd be nice to avoid possibly needlessly parsing if we don't update aliases but not a big deal
        setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
      } catch (NoNodeException e) {
        // /aliases.json will not always exist
      } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
        // note: aliases.json is required to be present
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred on Alias update", e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    /**
     * Update the internal aliases reference with a new one, provided that its ZK version has increased.
     *
     * @param newAliases the potentially newer version of Aliases
     * @return true if aliases have been updated to a new version, false otherwise
     */
    private boolean setIfNewer(Aliases newAliases) {
      assert newAliases.getZNodeVersion() >= 0;
      synchronized (this) {
        int cmp = Integer.compare(aliases.getZNodeVersion(), newAliases.getZNodeVersion());
        if (cmp < 0) {
          log.debug("Aliases: cmp={}, new definition is: {}", cmp, newAliases);
          aliases = newAliases;
          this.notifyAll();
          return true;
        } else {
          log.debug("Aliases: cmp={}, not overwriting ZK version.", cmp);
          assert cmp != 0 || Arrays.equals(aliases.toJSON(), newAliases.toJSON()) : aliases + " != " + newAliases;
          return false;
        }
      }
    }

  }

  private void notifyPropsWatchers(String collection, Map<String, String> properties) {
    try {
      notifications.submit(new PropsNotification(collection, properties));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection properties notifications for {}", collection, e);
      }
    }
  }

  private class PropsNotification implements Runnable {

    private final String collection;
    private final Map<String, String> collectionProperties;
    private final List<CollectionPropsWatcher> watchers = new ArrayList<>();

    private PropsNotification(String collection, Map<String, String> collectionProperties) {
      this.collection = collection;
      this.collectionProperties = collectionProperties;
      // guarantee delivery of notification regardless of what happens to collectionPropsObservers
      // while we wait our turn in the executor by capturing the list on creation.
      collectionPropsObservers.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        watchers.addAll(v.propStateWatchers);
        return v;
      });
    }

    @Override
    public void run() {
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      for (CollectionPropsWatcher watcher : watchers) {
        if (watcher.onStateChanged(collectionProperties)) {
          removeCollectionPropsWatcher(collection, watcher);
        }
      }
    }
  }

  private class CacheCleaner implements Runnable {
    public void run() {
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Executor shutdown will send us an interrupt
          break;
        }
        watchedCollectionProps.entrySet().removeIf(entry ->
            entry.getValue().cacheUntilNs < System.nanoTime() && !collectionPropsObservers.containsKey(entry.getKey()));
      }
    }
  }

  /**
   * Helper class that acts as both a {@link DocCollectionWatcher} and a {@link LiveNodesListener}
   * while wraping and delegating to a {@link org.apache.solr.common.cloud.CollectionStateWatcher}
   */
  private final class DocCollectionAndLiveNodesWatcherWrapper implements DocCollectionWatcher, LiveNodesListener {
    private final String collectionName;
    private final org.apache.solr.common.cloud.CollectionStateWatcher delegate;

    public int hashCode() {
      return collectionName.hashCode() * delegate.hashCode();
    }

    public boolean equals(Object other) {
      if (other instanceof DocCollectionAndLiveNodesWatcherWrapper) {
        DocCollectionAndLiveNodesWatcherWrapper that
            = (DocCollectionAndLiveNodesWatcherWrapper) other;
        return this.collectionName.equals(that.collectionName)
            && this.delegate.equals(that.delegate);
      }
      return false;
    }

    public DocCollectionAndLiveNodesWatcherWrapper(final String collectionName,
                                                   final org.apache.solr.common.cloud.CollectionStateWatcher delegate) {
      this.collectionName = collectionName;
      this.delegate = delegate;
    }

    @Override
    public boolean onStateChanged(DocCollection collectionState) {
      final boolean result = delegate.onStateChanged(ZkStateReader.this.liveNodes,
          collectionState);
      return result;
    }

    @Override
    public boolean onChange(SortedSet<String> newLiveNodes) {
      final DocCollection collection = watchedCollectionStates.get(collectionName);
      final boolean result = delegate.onStateChanged(newLiveNodes, collection);
      return result;
    }
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas) {
    return expectedShardsAndActiveReplicas(expectedShards, expectedReplicas, false);
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas, boolean exact) {
    return expectedShardsAndActiveReplicas(false, expectedShards, expectedReplicas, exact, null);
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(boolean justLeaders, int expectedShards, int expectedReplicas, boolean exact, Set<Replica> returnLeaders) {
    return (liveNodes, collectionState) -> {
      if (returnLeaders != null) returnLeaders.clear();
      if (collectionState == null)
        return false;
      Collection<Slice> activeSlices = collectionState.getActiveSlices();

      log.trace("active slices expected={} {} {} allSlices={}", expectedShards, activeSlices.size(), activeSlices, collectionState.getSlices());

      if (!exact) {
        if (activeSlices.size() < expectedShards) {
          return false;
        }
      } else {
        if (activeSlices.size() != expectedShards) {
          return false;
        }
      }

      if (expectedReplicas == 0 && !exact) {
        log.debug("0 replicas expected and found, return");
        return true;
      }

      int activeReplicas = 0;
      for (Slice slice : activeSlices) {
        Replica leader = slice.getLeader();
        log.trace("slice is {} and leader is {}", slice.getName(), leader);
        if (leader == null) {
          log.debug("slice={}", slice);
          return false;
        } else if (leader.getState() != Replica.State.ACTIVE) {
          return false;
        }
        if (returnLeaders != null) returnLeaders.add(leader);
        if (!justLeaders) {
          for (Replica replica : slice) {
            if (replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())) {
              activeReplicas++;
            }
          }
        }
        log.trace("slice is {} and active replicas is {}, expected {} liveNodes={}", slice.getName(), activeReplicas, expectedReplicas, liveNodes);
      }

      if (justLeaders) {
        return true;
      }
      if (!exact) {
        if (activeReplicas >= expectedReplicas) {
          return true;
        }
      } else {
        if (activeReplicas == expectedReplicas) {
          return true;
        }
      }

      return false;
    };
  }

  /* Checks both shard replcia consistency and against the control shard.
   * The test will be failed if differences are found.
   */
  public void checkShardConsistency(String collection) throws Exception {
    checkShardConsistency(collection, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(String collection, boolean verbose)
      throws Exception {

    Set<String> theShards = getCollection(collection).getSlicesMap().keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(collection, shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      System.err.println(failMessage);
      // MRM TODO: fail test if from tests

      throw new AssertionError(failMessage);
    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is
   * consistent.
   */
  protected String checkShardConsistency(String collection, String shard, boolean expectFailure, boolean verbose)
      throws Exception {


    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("\nCheck consistency of shard: " + shard);
    if (verbose) System.err.println("__________________________\n");
    int cnt = 0;

    DocCollection coll = getCollection(collection);

    Slice replicas = coll.getSlice(shard);

    Replica lastReplica = null;
    for (Replica replica : replicas) {

      //if (verbose) System.err.println("client" + cnt++);
      if (verbose) System.err.println("Replica: " + replica);
      try (SolrClient client = getHttpClient(replica.getCoreUrl())) {
        try {
          SolrParams query = params("q","*:*", "rows","0", "distrib","false", "tests","checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
          num = client.query(query).getResults().getNumFound();
        } catch (SolrException | SolrServerException e) {
          if (verbose) System.err.println("error contacting client: "
              + e.getMessage() + "\n");
          continue;
        }

        boolean live = false;
        String nodeName = replica.getNodeName();
        if (isNodeLive(nodeName)) {
          live = true;
        }
        if (verbose) System.err.println(" Live:" + live);
        if (verbose) System.err.println(" Count:" + num + "\n");

        boolean active = replica.getState() == Replica.State.ACTIVE;
        if (active && live) {
          if (lastNum > -1 && lastNum != num && failMessage == null) {
            failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastReplica.getCoreUrl() + " (previous client)" + " and got " + num + " from " + replica.getCoreUrl();

            if (!expectFailure || verbose) {
              System.err.println("######" + failMessage);
              SolrQuery query = new SolrQuery("*:*");
              query.set("distrib", false);
              query.set("fl", "id,_version_");
              query.set("rows", "100000");
              query.set("sort", "id asc");
              query.set("tests", "checkShardConsistency/showDiff");

              try (SolrClient lastClient = getHttpClient(lastReplica.getCoreUrl())) {
                SolrDocumentList lst1 = lastClient.query(query).getResults();
                SolrDocumentList lst2 = client.query(query).getResults();

                CloudInspectUtil.showDiff(lst1, lst2, lastReplica.getCoreUrl(), replica.getCoreUrl());
              }
            }

          }
          lastNum = num;
          lastReplica = replica;
        }
      }
    }
    return failMessage;

  }

  /**
   * Generates the correct SolrParams from an even list of strings.
   * A string in an even position will represent the name of a parameter, while the following string
   * at position (i+1) will be the assigned value.
   *
   * @param params an even list of strings
   * @return the ModifiableSolrParams generated from the given list of strings.
   */
  public static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  public Http2SolrClient getHttpClient(String baseUrl) {
    Http2SolrClient client = new Http2SolrClient.Builder(baseUrl)
        .idleTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    return client;
  }

  private class CloudCollectionsListenerConsumer implements Consumer<CloudCollectionsListener> {
    private final Set<String> oldCollections;
    private final Set<String> newCollections;

    public CloudCollectionsListenerConsumer(Set<String> oldCollections, Set<String> newCollections) {
      this.oldCollections = oldCollections;
      this.newCollections = newCollections;
    }

    @Override
    public void accept(CloudCollectionsListener listener) {
      if (log.isDebugEnabled()) log.debug("fire listeners {}", listener);
      notifications.submit(new ListenerOnChange(listener));
    }

    private class ListenerOnChange implements Runnable {
      private final CloudCollectionsListener listener;

      public ListenerOnChange(CloudCollectionsListener listener) {
        this.listener = listener;
      }

      @Override
      public void run() {
        log.debug("notify {}", newCollections);
        listener.onChange(oldCollections, newCollections);
      }
    }
  }

  private class ClusterPropsWatcher implements Watcher, Closeable {

    private final String path;

    ClusterPropsWatcher(String path) {
      this.path = path;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || EventType.None.equals(event.getType())) {
        return;
      }
      if (closed) return;
      loadClusterProperties();
    }

    @Override
    public void close() throws IOException {
      try {
        zkClient.removeAllWatches(path);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  private class PredicateMatcher {
    private CollectionStatePredicate predicate;
    private CountDownLatch latch;
    private AtomicReference<DocCollection> docCollection;

    public PredicateMatcher(CollectionStatePredicate predicate, CountDownLatch latch, AtomicReference<DocCollection> docCollection) {
      this.predicate = predicate;
      this.latch = latch;
      this.docCollection = docCollection;
    }

    public org.apache.solr.common.cloud.CollectionStateWatcher invoke() {
      return (n, c) -> {
        // if (isClosed()) return true;
        docCollection.set(c);
        boolean matches = predicate.matches(getLiveNodes(), c);
        if (matches)
          latch.countDown();

        return matches;
      };
    }
  }
}
