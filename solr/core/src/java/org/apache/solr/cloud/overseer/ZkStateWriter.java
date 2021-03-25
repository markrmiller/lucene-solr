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
package org.apache.solr.cloud.overseer;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

public class ZkStateWriter {
  // private static final long MAX_FLUSH_INTERVAL = TimeUnit.NANOSECONDS.convert(Overseer.STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkStateReader reader;
  private final Overseer overseer;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  private final Map<Long, Map> stateUpdates = new ConcurrentHashMap<>();

  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();

  private final Map<Long,String> idToCollection = new ConcurrentHashMap<>(128, 0.75f, 16);

  private Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,ReentrantLock> collLocks = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(128, 0.75f, 16);


  private AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();
  private Set<String> dirtyState = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public void enqueueStateUpdates(Map<Long,Map<Long,String>> replicaStates,  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates) {
    log.debug("enqueue state updates {}", replicaStates);
    for (Map.Entry<Long,Map<Long,String>> replicaStatesEntry : replicaStates.entrySet()) {

      this.stateUpdates.compute(replicaStatesEntry.getKey(),(k,v) -> {
        if (v != null) {
          v.putAll(replicaStatesEntry.getValue());
          return v;
        }
        v = new ConcurrentHashMap();
        v.putAll(replicaStatesEntry.getValue());
        return v;
      });
    }
    for (Map.Entry<Long,List<ZkStateWriter.StateUpdate>> sliceStatesEntry : sliceStates.entrySet()) {
      this.sliceStates.compute(sliceStatesEntry.getKey(), (k, v) -> {
        if (v != null) {
          v.addAll(sliceStatesEntry.getValue());
          return v;
        }
        v = new ArrayList<>();
        v.addAll(sliceStatesEntry.getValue());
        return v;
      });
    }
  }

  public void enqueueStructureChange(DocCollection docCollection) throws Exception {
    if (overseer.isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      throw new AlreadyClosedException();
    }
    try {

      log.trace("enqueue structure change docCollection={}", docCollection);

      String collectionName = docCollection.getName();
      ReentrantLock collLock = collLocks.compute(collectionName, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {

        docCollection = docCollection.copy();

        DocCollection currentCollection = cs.get(docCollection.getName());
        log.trace("zkwriter collection={}", docCollection);
        log.trace("zkwriter currentCollection={}", currentCollection);
        dirtyStructure.add(docCollection.getName());
        idToCollection.put(docCollection.getId(), docCollection.getName());

        if (currentCollection != null) {
          Map<Long,String> updates = new HashMap<>();
          this.stateUpdates.compute(docCollection.getId(), (k, v) -> {
            if (v == null) {
              return v;
            }
            updates.putAll(v);
            return v;
          });
          List<String> removeSlices = new ArrayList();
          List<Slice> updatedSlices = new ArrayList();
          for (Slice slice : currentCollection) {
            Slice updatedSlice = docCollection.getSlice(slice.getName());

            if (updatedSlice.getBool("remove", false)) {
              removeSlices.add(slice.getName());
            }

            if (updatedSlice.getState() != slice.getState()) {
              Map newProps = new HashMap(1);
              newProps.put(ZkStateReader.STATE_PROP, updatedSlice.getState().toString());
              updatedSlices.add(slice.copy(newProps));
            }
          }

          Map<String,Slice> newSlices = currentCollection.getSlicesCopy();
          for (Slice docCollectionSlice : docCollection.getSlices()) {
            Map<String,Replica> newReplicaMap = docCollectionSlice.getReplicasCopy();
            for (Replica replica : docCollectionSlice.getReplicas()) {
              String s = updates.get(replica.getInternalId());
              if (s != null) {
                Map newProps = new HashMap();
                if (s.equals("l")) {
                  newProps.put("leader", "true");
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
                } else {
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.shortStateToState(s).toString());
                  newProps.remove("leader");
                }
                Replica newReplica = replica.copy(newProps);
                newReplicaMap.put(replica.getName(), newReplica);
              }
            }

            Slice newDocCollectionSlice = docCollectionSlice.copy(newReplicaMap);

            newSlices.putIfAbsent(newDocCollectionSlice.getName(), newDocCollectionSlice);
          }

          for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }

          Map newDocProps = new HashMap(currentCollection.getProperties());
          newDocProps.forEach((k, v) -> newDocProps.putIfAbsent(k, v));
          DocCollection newCollection = currentCollection.copyWithSlices(newSlices, newDocProps);
          log.debug("zkwriter newCollection={}", newCollection);
          cs.put(currentCollection.getName(), newCollection);

        } else {
          Map newDocProps = new HashMap(docCollection.getProperties());
          Map<Long,String> updates = new HashMap<>();
          this.stateUpdates.compute(docCollection.getId(), (k, v) -> {
            if (v == null) {
              return v;
            }
            updates.putAll(v);
            return v;
          });

          newDocProps.remove("pullReplicas");
          newDocProps.remove("replicationFactor");
          newDocProps.remove("maxShardsPerNode");
          newDocProps.remove("nrtReplicas");
          newDocProps.remove("tlogReplicas");
          newDocProps.remove("numShards");

          Map<String,Slice> newSlices = docCollection.getSlicesCopy();
          List<String> removeSlices = new ArrayList();
          for (Slice slice : docCollection) {

            if (slice.getProperties().get("remove") != null) {
              removeSlices.add(slice.getName());
            }
            if (updates != null) {

              for (Replica replica : slice.getReplicas()) {
                String s = updates.get(replica.getInternalId());
                if (s != null) {
                  Map<String,Replica> newReplicaMap = slice.getReplicasCopy();
                  Map newProps = new HashMap(2);

                  if (s.equals("l")) {
                    newProps.put("leader", "true");
                    newProps.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
                  } else {
                    newProps.put(ZkStateReader.STATE_PROP, Replica.State.shortStateToState(s).toString());
                    newProps.remove("leader");
                  }
                  Replica newReplica = replica.copy(newProps);
                  newReplicaMap.put(newReplica.getName(), newReplica);
                  Slice newSlice = slice.copy(newReplicaMap);
                  newSlices.put(newSlice.getName(), newSlice);
                }
              }
            }
          } for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }
          cs.put(docCollection.getName(), docCollection.copyWithSlices(newSlices, newDocProps));
        }

      } finally {
        collLock.unlock();
      }

      //      } else {
      //        log.trace("enqueue state change states={}", collStateUpdates);
      //        for (Map.Entry<String,ConcurrentHashMap<String,ZkStateWriter.StateUpdate>> entry : collStateUpdates.entrySet()) {
      //          ReentrantLock collLock = collLocks.compute(entry.getKey(), (s, reentrantLock) -> {
      //            if (reentrantLock == null) {
      //              return new ReentrantLock();
      //            }
      //            return reentrantLock;
      //          });
      //          collLock.lock();
      //          try {
      //            String collectionId = entry.getKey();
      //            long collId = Long.parseLong(collectionId);
      //            String collection = idToCollection.get(collId);
      //
      //
      //
      //            ConcurrentHashMap updates;
      //            DocCollection docColl = cs.get(collection);
      //            String csVersion;
      //            if (docColl != null) {
      //
      //              updates = stateUpdates.get(collId);
      //              if (updates == null) {
      //                updates = new ConcurrentHashMap();
      //                stateUpdates.put(collId, updates);
      //              }
      //
      //              int clusterStateVersion = docColl.getZNodeVersion();
      //              csVersion = Integer.toString(clusterStateVersion);
      //              for (StateUpdate state : entry.getValue().values()) {
      //                if (state.sliceState != null) {
      //                  Slice slice = docColl.getSlice(state.sliceName);
      //                  if (slice != null) {
      //                    slice.setState(Slice.State.getState(state.sliceState));
      //                    slice.getProperties().put("state", state.sliceState);
      //                  }
      //                  dirtyStructure.add(collection);
      //                  continue;
      //                }
      //
      //                Replica replica = docColl.getReplicaById(state.id);
      //                log.trace("found existing collection name={}, look for replica={} found={}", collection, state.id, replica);
      //                if (replica != null) {
      //
      //                  log.trace("zkwriter publish state={} replica={}", state.state, replica.getName());
      //                  if (state.state.equals("l")) {
      //
      //                    log.trace("set leader {}", replica);
      //
      //                    Slice slice = docColl.getSlice(replica.getSlice());
      //
      //                    Map<String, Replica> replicasMap = slice.getReplicasCopy();
      //                    Map properties = new HashMap(replica.getProperties());
      //
      //                    properties.put("leader",  "true");
      //                    properties.put("state", Replica.State.ACTIVE);
      //
      //                    for (Replica r : replicasMap.values()) {
      //                      if (replica.getName().equals(r.getName())) {
      //                        continue;
      //                      }
      //                      log.trace("process non leader {} {}", r, r.getProperty(ZkStateReader.LEADER_PROP));
      //                      if ("true".equals(r.getProperties().get(ZkStateReader.LEADER_PROP))) {
      //                        log.debug("remove leader prop {}", r);
      //                        Map<String,Object> props = new HashMap<>(r.getProperties());
      //                        props.remove(ZkStateReader.LEADER_PROP);
      //                        Replica newReplica = new Replica(r.getName(), props, collection, docColl.getId(), r.getSlice(), r.getBaseUrl());
      //                        replicasMap.put(r.getName(), newReplica);
      //                      }
      //                    }
      //
      //                    Replica newReplica = new Replica(replica.getName(), properties, collection, docColl.getId(), replica.getSlice(), replica.getBaseUrl());
      //
      //                    replicasMap.put(replica.getName(), newReplica);
      //
      //                    Slice newSlice = slice.copy(replicasMap);
      //
      //                    Map<String,Slice> newSlices = docColl.getSlicesCopy();
      //                    newSlices.put(slice.getName(), newSlice);
      //
      //                    log.trace("add new slice leader={} {} {}", newSlice.getLeader(), newSlice, docColl);
      //
      //                    DocCollection newDocCollection = docColl.copyWithSlices(newSlices);
      //                    cs.put(collection, newDocCollection);
      //                    docColl = newDocCollection;
      //                    updates.put(replica.getInternalId(), "l");
      //                    dirtyState.add(collection);
      //                  } else {
      //                    String setState = Replica.State.shortStateToState(state.state).toString();
      //                    Replica.State s = Replica.State.getState(setState);
      //
      //                    log.trace("set state {} {}", state, replica);
      //
      //                    Slice slice = docColl.getSlice(replica.getSlice());
      //                    Map<String,Replica> replicasMap = slice.getReplicasCopy();
      //                    Map properties = new HashMap(replica.getProperties());
      //
      //                    properties.put("state", s.toString());
      //                    properties.remove(ZkStateReader.LEADER_PROP);
      //
      //                    Replica newReplica = new Replica(replica.getName(), properties, collection, docColl.getId(), replica.getSlice(), replica.getBaseUrl());
      //
      //                    replicasMap.put(replica.getName(), newReplica);
      //
      //                    Slice newSlice = slice.copy(replicasMap);
      //
      //                    Map<String,Slice> newSlices = docColl.getSlicesCopy();
      //                    newSlices.put(slice.getName(), newSlice);
      //
      //                    log.trace("add new slice leader={} {}", newSlice.getLeader(), newSlice);
      //
      //                    DocCollection newDocCollection = docColl.copyWithSlices(newSlices);
      //                    cs.put(collection, newDocCollection);
      //                    docColl = newDocCollection;
      //                    updates.put(replica.getInternalId(), state.state);
      //                    dirtyState.add(collection);
      //                  }
      //                } else {
      //                  log.debug("Could not find replica id={} in {} {}", state.id, docColl.getReplicaByIds(), docColl.getReplicas());
      //                }
      //              }
      //            } else {
      //              updates = stateUpdates.get(collId);
      //              if (updates == null) {
      //                updates = new ConcurrentHashMap();
      //                stateUpdates.put(collId, updates);
      //              }
      //
      //              for (StateUpdate state : entry.getValue().values()) {
      //                log.debug("Could not find existing collection name={}", collection);
      //                String setState = Replica.State.shortStateToState(state.state).toString();
      //                if (setState.equals("l")) {
      //                  updates.put(state.id.substring(state.id.indexOf('-') + 1), "l");
      //                  dirtyState.add(collection);
      //                } else {
      //                  Replica.State s = Replica.State.getState(setState);
      //                  updates.put(state.id.substring(state.id.indexOf('-') + 1), Replica.State.getShortState(s));
      //                  dirtyState.add(collection);
      //                }
      //              }
      //              log.debug("version for state updates 0");
      //              csVersion = "0";
      //            }
      //
      //            if (dirtyState.contains(collection)) {
      //              updates.put("_cs_ver_", csVersion);
      //            }
      //
      //          } finally {
      //            collLock.unlock();
      //          }
      //        }

    } catch (Exception e) {
      log.error("Exception while queuing update", e);
      throw e;
    }
  }

  public Integer lastWrittenVersion(String collection) {
    DocCollection col = cs.get(collection);
    if (col == null) {
      return 0;
    }
    return col.getZNodeVersion();
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */

  public Future writePendingUpdates(String collection) {
    if (overseer.isClosed()) throw new AlreadyClosedException();
    return ParWork.getRootSharedExecutor().submit(() -> {
      MDCLoggingContext.setNode(overseer.getCoreContainer().getZkController().getNodeName());
      do {
        try {
          write(collection);
          break;
        } catch (KeeperException.BadVersionException e) {
          log.warn("hit bad version trying to write state.json, trying again ...");
        } catch (Exception e) {
          log.error("write pending failed", e);
          break;
        }

      } while (!overseer.isClosed() && !overseer.getZkStateReader().getZkClient().isClosed());
    });
  }

  private void write(String coll) throws KeeperException.BadVersionException {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", coll);
    }
    AtomicReference<KeeperException.BadVersionException> badVersionException = new AtomicReference();

    log.debug("process collection {}", coll);
    ReentrantLock collLock = collLocks.compute(coll, (s, reentrantLock) -> {
      if (reentrantLock == null) {
        return new ReentrantLock();
      }
      return reentrantLock;
    });
    collLock.lock();
    try {

      DocCollection collection = cs.get(coll);

      if (collection == null) {
        return;
      }

      collection = cs.get(coll);

      if (collection == null) {
        return;
      }

      if (log.isTraceEnabled()) log.trace("check collection {} {} {}", collection, dirtyStructure, dirtyState);

      //  collState.throttle.minimumWaitBetweenActions();
      //  collState.throttle.markAttemptingAction();
      String name = collection.getName();
      String path = ZkStateReader.getCollectionPath(collection.getName());
      String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());

      if (log.isTraceEnabled()) log.trace("process {}", collection);
      try {

        if (dirtyStructure.contains(name)) {
          if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());

          byte[] data = Utils.toJSON(singletonMap(name, collection));

          if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} stateupdates={} col={} ", collection.getZNodeVersion(), data.length, stateUpdates.get(coll), collection);

          Integer finalVersion = collection.getZNodeVersion();

          if (reader == null) {
            log.error("read not initialized in zkstatewriter");
          }
          if (reader.getZkClient() == null) {
            log.error("zkclient not initialized in zkstatewriter");
          }

          Stat stat;
          try {

            stat = reader.getZkClient().setData(path, data, finalVersion, true, false);
            collection.setZnodeVersion(stat.getVersion());
            dirtyStructure.remove(collection.getName());
            if (log.isDebugEnabled()) log.debug("set new version {} {}", collection.getName(), stat.getVersion());
          } catch (KeeperException.NoNodeException e) {
            log.debug("No node found for state.json", e);

          } catch (KeeperException.BadVersionException bve) {
            stat = reader.getZkClient().exists(path, null, false, false);
            log.info("Tried to update state.json for {} with bad version {} found={} \n {}", coll, finalVersion, stat != null ? stat.getVersion() : "null", collection);
            DocCollection docColl = reader.getCollectionLive(coll);
            idToCollection.put(docColl.getId(), docColl.getName());
            cs.put(coll, docColl);

            throw bve;
          }

          reader.getZkClient().setData(pathSCN, null, -1, true, false);

          Map updates = stateUpdates.get(collection.getId());
          if (updates != null) {
            // TODO: clearing these correctly is tricky
            updates.clear();
            writeStateUpdates(collection, Collections.emptyMap());
          }

        } else if (dirtyState.contains(collection.getName())) {
          Map updates = stateUpdates.get(collection.getName());
          if (updates != null) {
            try {
              writeStateUpdates(collection, updates);
            } catch (Exception e) {
              log.error("exception writing state updates", e);
            }
          }
        }

      } catch (KeeperException.BadVersionException bve) {
        badVersionException.set(bve);
      } catch (InterruptedException | AlreadyClosedException e) {
        log.info("We have been closed or one of our resources has, bailing {}", e.getClass().getSimpleName() + ":" + e.getMessage());
        throw new AlreadyClosedException(e);

      } catch (Exception e) {
        log.error("Failed processing update=" + collection, e);
      }

    } finally {
      collLock.unlock();
    }

    if (badVersionException.get() != null) {
      throw badVersionException.get();
    }

  }

  private void writeStateUpdates(DocCollection collection, Map updates) throws KeeperException, InterruptedException {
    if (updates.size() == 0) {
      return;
    }
    String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
    log.trace("write state updates for collection {} ver={} {}", collection.getName(), updates.get("_cs_ver_"), updates);
    try {
      reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(updates), -1, true, false);
    } catch (KeeperException.NoNodeException e) {
      if (log.isDebugEnabled()) log.debug("No node found for state.json", e);
      // likely deleted
    }
    dirtyState.remove(collection.getName());
  }

  public ClusterState getClusterstate(String collection) {

    Map<String,DocCollection> map;
    if (collection != null) {

      map = new HashMap<>(1);
      DocCollection coll = cs.get(collection);
      if (coll != null) {
        map.put(collection, coll.copy());
      }

    } else {
      map = new HashMap<>(cs.keySet().size());
      cs.forEach((s, docCollection) -> map.put(s, docCollection.copy()));
    }

    return ClusterState.getRefCS(map, -2);

  }

  public Long getIdForCollection(String collection) {
    ReentrantLock collectionLock = collLocks.get(collection);
    if (collectionLock == null) {
      return null;
    }
    collectionLock.lock();
    try {
      return cs.get(collection).getId();
    } finally {
      collectionLock.unlock();
    }
  }

  public Set<String> getDirtyStateCollections() {
    return dirtyState;
  }

  public Set<String> getDirtyStructureCollections() {
    return dirtyStructure;
  }


  public void removeCollection(String collection) {
    log.debug("Removing collection from zk state {}", collection);
    try {
      ReentrantLock collLock = collLocks.compute(collection, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {
        Long id = null;
        for (Map.Entry<Long,String> entry : idToCollection.entrySet()) {
          if (entry.getValue().equals(collection)) {
            id = entry.getKey();
            break;
          }
        }
        if (id != null) {
          idToCollection.remove(id);
        }
        stateUpdates.remove(collection);
        DocCollection doc = cs.get(collection);

        if (doc != null) {
          List<Replica> replicas = doc.getReplicas();
          for (Replica replica : replicas) {
            overseer.getCoreContainer().getZkController().clearCachedState(replica.getName());
          }
          idToCollection.remove(doc.getId());
        }

        cs.remove(collection);
        assignMap.remove(collection);
        dirtyStructure.remove(collection);
        dirtyState.remove(collection);

      } finally {
        collLock.unlock();
      }
    } catch (Exception e) {
      log.error("Exception removing collection", e);

    }
  }

  public long getHighestId(String collection) {
    long id = ID.incrementAndGet();
    idToCollection.put(id,collection);
    return id;
  }

  public synchronized int getReplicaAssignCnt(String collection, String shard) {
    DocAssign docAssign = assignMap.get(collection);
    if (docAssign == null) {
      docAssign = new DocAssign();
      docAssign.name = collection;
      assignMap.put(docAssign.name, docAssign);


      int id = docAssign.replicaAssignCnt.incrementAndGet();
      log.debug("assign id={} for collection={} slice={}", id, collection, shard);
      return id;
    }

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.debug("assign id={} for collection={} slice={}", id, collection, shard);
    return id;
  }

  public void init() {
    log.info("ZkStateWriter Init - A new Overseer in charge or we are back baby");
    try {
      overseer.getCoreContainer().getZkController().clearStatePublisher();
      ClusterState readerState = reader.getClusterState();
      Set<String> collectionNames = new HashSet<>(readerState.getCollectionsMap().size());
      if (readerState != null) {
        readerState.forEachCollection( docCollection -> collectionNames.add(docCollection.getName()));
      }

      long[] highId = new long[1];
      collectionNames.forEach(collectionName -> {

        ReentrantLock collLock = collLocks.compute(collectionName, (s, reentrantLock) -> {
          if (reentrantLock == null) {
            return new ReentrantLock();
          }
          return reentrantLock;
        });
        collLock.lock();
        try {

          DocCollection docCollection = reader.getCollectionLive(collectionName);

          Map latestStateUpdates = docCollection.getStateUpdates();

          stateUpdates.compute(docCollection.getId(), (k,v) -> {
            if (v == null) {
              return latestStateUpdates;
            }

            Integer zkVersion = (Integer) v.get("_ver_");
            if (zkVersion == null) {
              zkVersion = -1;
            }
            if (docCollection.getStateUpdatesZkVersion() > zkVersion) {
              return latestStateUpdates;
            }
            return v;
          });

          cs.put(collectionName, docCollection);


          if (docCollection.getId() > highId[0]) {
            highId[0] = docCollection.getId();
          }

          idToCollection.put(docCollection.getId(), docCollection.getName());

          DocAssign docAssign = new DocAssign();
          docAssign.name = docCollection.getName();
          assignMap.put(docAssign.name, docAssign);
          int max = 1;
          Collection<Slice> slices = docCollection.getSlices();
          for (Slice slice : slices) {
            Collection<Replica> replicas = slice.getReplicas();

            for (Replica replica : replicas) {
              Matcher matcher = Assign.pattern.matcher(replica.getName());
              if (matcher.matches()) {
                int val = Integer.parseInt(matcher.group(1));
                max = Math.max(max, val);
              }
            }
          }
          docAssign.replicaAssignCnt.set(max);
        } finally {
          collLock.unlock();
        }
      });

      ID.set(highId[0]);

      if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
    } catch (Exception e) {
      log.error("Exception in ZkStateWriter init", e);
    }
  }

  public Set<String> getCollections() {
    return cs.keySet();
  }

  public void writeStateUpdates(Set<Long> collIds) {
    for (Long collId : collIds) {
      String collection = idToCollection.get(collId);
      if (collection != null) {
        log.debug("Write out state updates for {} updates={}", collection, stateUpdates);
        String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection);
        try {
          reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(stateUpdates.get(collId)), -1, (rc, path1, ctx, stat) -> {
            if (rc != 0) {
              log.error("got zk error deleting path {} {}", path1, rc);
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              log.error("Exception deleting znode path=" + path1, e);
            }
          }, "stateupdates");
        } catch (Exception e) {
          log.error("Exception writing out state updates async", e);
        }
      }
    }
  }

  private static class DocAssign {
    String name;
    private AtomicInteger replicaAssignCnt = new AtomicInteger();
  }

  public static class StateUpdate {
    public volatile String id;
    public volatile String state;
    public volatile String sliceState;
    public volatile String sliceName;
    public volatile String nodeName;
  }

}

