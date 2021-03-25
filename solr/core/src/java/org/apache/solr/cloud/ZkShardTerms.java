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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for interact with a ZK term node.
 * Each ZK term node relates to a shard of a collection and have this format (in json)
 * <p>
 * <code>
 * {
 *   "replicaNodeName1" : 1,
 *   "replicaNodeName2" : 2,
 *   ..
 * }
 * </code>
 * <p>
 * The values correspond to replicas are called terms.
 * Only replicas with highest term value are considered up to date and be able to become leader and serve queries.
 * <p>
 * Terms can only updated in two strict ways:
 * <ul>
 * <li>A replica sets its term equals to leader's term
 * <li>The leader increase its term and some other replicas by 1
 * </ul>
 * This class should not be reused after {@link org.apache.zookeeper.Watcher.Event.KeeperState#Expired} event
 */
public class ZkShardTerms implements Closeable, Watcher, DoNotWrap {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String collection;
  private final String shard;
  private final String znodePath;
  private final SolrZkClient zkClient;

  private volatile boolean disableRemoveWatches = false;

  private final Map<String, CoreTermWatcher> listeners = new ConcurrentHashMap<>();

  private final AtomicReference<ShardTerms> terms = new AtomicReference<>();
  private volatile boolean checkAgain = false;
  private volatile boolean running;

  protected final ReentrantLock ourLock = new ReentrantLock(false);
  private volatile boolean closed;

  @Override
  public String toString() {
    return "ZkShardTerms{" + "terms=" + terms.get() + '}';
  }

  @Override
  public void process(WatchedEvent event) {
    if (!Event.EventType.NodeDataChanged.equals(event.getType())) {
      return;
    }

    if (closed || zkClient.isClosed()) {
      return;
    }
    ourLock.lock();
    try {
      if (running) {
        checkAgain = true;
      } else {
        running = true;

        ParWork.getRootSharedExecutor().submit(() -> {
          try {
            do {
              refresh();

              ourLock.lock();
              try {
                if (checkAgain) {
                  checkAgain = false;
                } else {
                  running = false;
                  break;
                }
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

  /**
   * Listener of a core for shard's term change events
   */
  abstract static class CoreTermWatcher implements Closeable {
    /**
     * Invoked with a Terms instance after update. <p>
     * Concurrent invocations of this method is not allowed so at a given time only one thread
     * will invoke this method.
     * <p>
     * <b>Note</b> - there is no guarantee that the terms version will be strictly monotonic i.e.
     * an invocation with a newer terms version <i>can</i> be followed by an invocation with an older
     * terms version. Implementations are required to be resilient to out-of-order invocations.
     *
     * @param terms instance
     * @return true if the listener wanna to be triggered in the next time
     */
    abstract boolean onTermChanged(ShardTerms terms);
  }

  public ZkShardTerms(String collection, String shard, SolrZkClient zkClient) throws IOException, KeeperException {
    this.znodePath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    this.collection = collection;
    this.shard = shard;
    this.zkClient = zkClient;
    createWatcher();

    refresh();

    assert ObjectReleaseTracker.track(this);
  }

  public void createWatcher() {

    try {
      zkClient.addWatch(znodePath, this, AddWatchMode.PERSISTENT, true, true);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeWatcher() {
    try {
      zkClient.removeAllWatches(znodePath);
    } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }
  /**
   * Ensure that leader's term is higher than some replica's terms
   * @param leader coreNodeName of leader
   * @param replicasNeedingRecovery set of replicas in which their terms should be lower than leader's term
   */
  public void ensureTermsIsHigher(String leader, Set<String> replicasNeedingRecovery) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("ensureTermsIsHigher leader={} replicasNeedingRecvoery={}", leader, replicasNeedingRecovery);
    if (replicasNeedingRecovery.isEmpty()) return;
    ShardTerms newTerms;
    while( (newTerms = terms.get().increaseTerms(leader, replicasNeedingRecovery)) != null) {
      if (saveTerms(newTerms)) return;
    }
  }

  public ShardTerms getShardTerms() {
    return terms.get();
  }
  /**
   * Can this replica become leader?
   * @param coreNodeName of the replica
   * @return true if this replica can become leader, false if otherwise
   */
  public boolean canBecomeLeader(String coreNodeName) {
    return terms.get().canBecomeLeader(coreNodeName);
  }

  /**
   * Should leader skip sending updates to this replica?
   * @param coreNodeName of the replica
   * @return true if this replica has term equals to leader's term, false if otherwise
   */
  public boolean skipSendingUpdatesTo(String coreNodeName) {
    if (log.isDebugEnabled()) log.debug("skipSendingUpdatesTo {} {}", coreNodeName, terms);

    return !terms.get().haveHighestTermValue(coreNodeName);
  }

  /**
   * Did this replica registered its term? This is a sign to check f
   * @param coreNodeName of the replica
   * @return true if this replica registered its term, false if otherwise
   */
  public boolean registered(String coreNodeName) {
    return terms.get().getTerm(coreNodeName) != null;
  }

  public void close() {
    // no watcher will be registered
    //isClosed.set(true);
    closed = true;
    listeners.values().forEach(coreTermWatcher -> IOUtils.closeQuietly(coreTermWatcher));
    listeners.clear();

    removeWatcher();

    assert ObjectReleaseTracker.release(this);
  }

  public void disableRemoveWatches() {
    this.disableRemoveWatches = true;
  }

  // package private for testing, only used by tests
  Map<String, Long> getTerms() {
    return new HashMap<>(terms.get().getTerms());
  }

  /**
   * Add a listener so the next time the shard's term get updated, listeners will be called
   */
  void addListener(String core, CoreTermWatcher listener) {
    listeners.put(core, listener);
  }

  /**
   * Remove the coreNodeName from terms map and also remove any expired listeners
   * @return Return true if this object should not be reused
   */
  boolean removeTermFor(String name) throws KeeperException, InterruptedException {
    IOUtils.closeQuietly(listeners.remove(name));

    return removeTerm(name) || getNumListeners() == 0;
  }

  // package private for testing, only used by tests
  // return true if this object should not be reused
  boolean removeTerm(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().removeTerm(coreNodeName)) != null) {
      try {
        if (saveTerms(newTerms)) {
          return false;
        }
      } catch (KeeperException.NoNodeException e) {
        return true;
      }
    }
    return true;
  }

  /**
   * Register a replica's term (term value will be 0).
   * If a term is already associate with this replica do nothing
   * @param coreNodeName of the replica
   * @return
   */
  ShardTerms registerTerm(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ((newTerms = terms.get().registerTerm(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
    return newTerms;
  }

  /**
   * Set a replica's term equals to leader's term, and remove recovering flag of a replica.
   * This call should only be used by {@link org.apache.solr.common.params.CollectionParams.CollectionAction#FORCELEADER}
   * @param coreNodeName of the replica
   */
  public void setTermEqualsToLeader(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().setTermEqualsToLeader(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  public void setTermToZero(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().setTermToZero(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as recovering
   */
  public void startRecovering(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().startRecovering(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as finished recovering
   */
  public void doneRecovering(String coreNodeName) throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().doneRecovering(coreNodeName)) != null) {
      if (saveTerms(newTerms)) break;
    }
  }

  public boolean isRecovering(String name) {
    return terms.get().isRecovering(name);
  }

  /**
   * When first updates come in, all replicas have some data now,
   * so we must switch from term 0 (registered) to 1 (have some data)
   */
  public void ensureHighestTermsAreNotZero() throws KeeperException, InterruptedException {
    ShardTerms newTerms;
    while ( (newTerms = terms.get().ensureHighestTermsAreNotZero()) != null) {
      if (log.isDebugEnabled()) log.debug("Terms are at " + terms.get());
      if (saveTerms(newTerms)) break;
    }
  }

  public long getHighestTerm() {
    return terms.get().getMaxTerm();
  }

  public long getTerm(String coreNodeName) {
    Long term = terms.get().getTerm(coreNodeName);
    return term == null? -1 : term;
  }

  // package private for testing, only used by tests
  int getNumListeners() {
    return listeners.size();
  }

  /**
   * Set new terms to ZK, the version of new terms must match the current ZK term node
   * @param newTerms to be set
   * @return true if terms is saved successfully to ZK, false if otherwise
   * @throws KeeperException.NoNodeException correspond ZK term node is not created
   */
  private boolean saveTerms(ShardTerms newTerms) throws KeeperException, InterruptedException {
    log.info("save terms {} {}", shard, newTerms);
    byte[] znodeData = Utils.toJSON(newTerms);

    try {
      Stat stat = zkClient.setData(znodePath, znodeData, newTerms.getVersion(), true);
      ShardTerms newShardTerms = new ShardTerms(newTerms, stat.getVersion());
      setNewTerms(newShardTerms);
      log.debug("Successful update of terms at {} to {}", znodePath, newTerms);
      return true;
    } catch (KeeperException.BadVersionException e) {
      int foundVersion = -1;
      Stat stat = zkClient.exists(znodePath, null);
      if (stat != null) {
        foundVersion = stat.getVersion();
      }
      log.info("Failed to save terms, version is not a match, retrying version={} found={}", newTerms.getVersion(), foundVersion);

      refreshTerms(foundVersion);
    }
    return false;
  }

  /**
   * Fetch latest terms from ZK
   */
  public void refreshTerms(int version) throws KeeperException {

    ShardTerms newTerms;

    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, null, stat, true);
      if (data == null) {
        setNewTerms(new ShardTerms(new ConcurrentHashMap(), -1));
        return;
      }
      Map<String,Long> values = Collections.unmodifiableMap(new HashMap<>((Map<String,Long>) Utils.fromJSON(data)));
      log.debug("refresh shard terms to zk version {} values={}", stat.getVersion(), values);
      newTerms = new ShardTerms(values, stat.getVersion());
    } catch (KeeperException.NoNodeException e) {
      log.info("No node found for shard terms {} znodepath={}", e.getPath(), znodePath);
      // we have likely been deleted
      return;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    }

    setNewTerms(newTerms);
  }

  /**
   * Retry register a watcher to the correspond ZK term node
   */
  public void refresh() throws KeeperException {
    refreshTerms(-1);
  }

  /**
   * Atomically update {@link ZkShardTerms#terms} and call listeners
   * @param newTerms to be set
   */
  private void setNewTerms(ShardTerms newTerms) {

    boolean isChanged = false;
    int cnt = 0;
    for (;;)  {
      cnt++;
      log.debug("set new terms {} {}", newTerms, cnt);

      if (log.isDebugEnabled()) log.debug("set new terms {} {}", newTerms, cnt);
      ShardTerms terms = this.terms.get();
      if (terms == null || newTerms.getVersion() > terms.getVersion())  {
        if (this.terms.compareAndSet(terms, newTerms))  {
          log.debug("terms set");
          isChanged = true;
          break;
        }
      } else  {
        break;
      }
    }

    if (isChanged) onTermUpdates(newTerms);
  }

  private void onTermUpdates(ShardTerms newTerms) {
    try {
      listeners.values().forEach(coreTermWatcher -> coreTermWatcher.onTermChanged(newTerms));
    } catch (Exception e) {
      log.error("Error calling shard term listener", e);
    }
  }
}
