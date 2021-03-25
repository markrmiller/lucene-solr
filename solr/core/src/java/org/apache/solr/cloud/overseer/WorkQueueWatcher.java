package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WorkQueueWatcher extends QueueWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Overseer overseer;

  public WorkQueueWatcher(Overseer overseer, CoreContainer cc) throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_QUEUE);
    this.overseer = overseer;
  }

  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;

    zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);
    ourLock.lock();
    try {
      startItems = getItems();

      log.info("Overseer found entries on start {} {}", startItems, path);

      if (startItems.size() > 0) {
        processQueueItems(startItems, true, weAreReplacement);
      }
    } finally {
      ourLock.unlock();
    }
  }

  protected List<String> getItems() {
    try {

      log.debug("get items from Overseer work queue {}", path);

      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);

      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      return items;
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper session expired");
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {

      log.error("Unexpected error in Overseer state update loop", e);

      log.error("Unexpected error in Overseer state update loop", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (Event.EventType.None.equals(event.getType())) {
      return;
    }
    if (this.closed || zkController.getZkClient().isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      return;
    }

    ourLock.lock();
    try {
      try {
        List<String> items = getItems();
        if (items.size() > 0) {
          processQueueItems(items, false, false);
        }
      } catch (AlreadyClosedException e) {

      } catch (Exception e) {
        log.error("Exception during overseer queue queue processing", e);
      }
    } finally {
      ourLock.unlock();
    }

  }

  @Override
  protected void processQueueItems(List<String> items, boolean onStart, boolean weAreReplacement) {
    //if (closed) return;

    ourLock.lock();
    List<String> fullPaths = new ArrayList<>(items.size());
    String forceWrite = null;
    boolean wroteUpdates = false;
    try {

      log.debug("Found state update queue items {}", items);

      for (String item : items) {
        fullPaths.add(path + "/" + item);
      }

      Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

      Map<Long,Map<Long,String>> replicaStates = new HashMap<>();
      Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new HashMap<>();

      Set<Long> collIds = new HashSet<>();
      for (byte[] item : data.values()) {
        final ZkNodeProps message = ZkNodeProps.load(item);

        log.debug("add state update {}", message);

        final String op = message.getStr(StatePublisher.OPERATION);
        message.getProperties().remove(StatePublisher.OPERATION);
        OverseerAction overseerAction = OverseerAction.get(op);
        switch (overseerAction) {
          case STATE:
            for (Map.Entry<String,Object> entry : message.getProperties().entrySet()) {
              try {

                OverseerAction oa = OverseerAction.get(entry.getKey());

                if (OverseerAction.RECOVERYNODE.equals(oa) || OverseerAction.DOWNNODE.equals(oa)) {
                  if (OverseerAction.DOWNNODE.equals(oa) && onStart && !weAreReplacement) {
                    continue;
                  }
                  ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();
                  if (OverseerAction.DOWNNODE.equals(oa)) {
                    update.state = Replica.State.getShortState(Replica.State.DOWN);
                  } else if (OverseerAction.RECOVERYNODE.equals(oa)) {
                    update.state = Replica.State.getShortState(Replica.State.RECOVERING);
                  }
                  update.nodeName = (String) entry.getValue();

                  Set<String> collections = overseer.getZkStateWriter().getCollections();

                  for (String collection : collections) {
                    ClusterState cs = overseer.getZkStateWriter().getClusterstate(collection);
                    if (cs != null) {
                      DocCollection docCollection = cs.getCollection(collection);
                      if (docCollection != null) {
                        List<Replica> replicas = docCollection.getReplicas();
                        for (Replica replica : replicas) {
                          if (replica.getNodeName().equals(update.nodeName)) {
                            Map<Long,String> updates = replicaStates.get(replica.getCollectionId());
                            if (updates == null) {
                              updates = new ConcurrentHashMap<>();
                              replicaStates.put(replica.getCollectionId(), updates);
                            }
                            updates.put(replica.getInternalId(), update.state);
                          }
                        }
                      }
                    }
                  }
                  continue;
                }

                String id = entry.getKey();
                String stateString = (String) entry.getValue();
                long collId = Long.parseLong(id.substring(0, id.indexOf('-')));

                Map<Long,String> updates = replicaStates.get(collId);
                if (updates == null) {
                  updates = new ConcurrentHashMap<>();
                  collIds.add(collId);
                  replicaStates.put(collId, updates);
                }
                long replicaId = Long.parseLong(id.substring(id.indexOf('-') + 1));
                log.debug("add state update {} {} for collection {}", replicaId, stateString, collId);
                updates.put(replicaId, stateString);

              } catch (Exception e) {
                log.error("Overseer state update queue processing failed", e);
              }
            }
            break;
          case UPDATESHARDSTATE:
            ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();

            update.sliceState = message.getStr("state");
            update.sliceName = message.getStr("slice");
            update.id = message.getStr("id");

            List<ZkStateWriter.StateUpdate> updates = sliceStates.get(overseer.getId());
            if (updates == null) {
              updates = new ArrayList<>();
              sliceStates.put(Long.parseLong(overseer.getId()), updates);
            }
            updates.add(update);

            break;

          default:
            throw new RuntimeException("unknown operation:" + op + " contents:" + message.getProperties());
        }
      }

      overseer.getZkStateWriter().enqueueStateUpdates(replicaStates, sliceStates);

      overseer.getZkStateWriter().writeStateUpdates(collIds);

      wroteUpdates = true;
    } finally {
      try {
        if (fullPaths.size() > 0 && wroteUpdates) {
          if (!zkController.getZkClient().isClosed()) {
            try {
              zkController.getZkClient().delete(fullPaths, true);
            } catch (Exception e) {
              log.warn("Failed deleting processed items", e);
            }
          }
        }

      } finally {
        ourLock.unlock();
      }
    }
  }

  @Override
  protected void doneWithCurrentQueue() {

  }
}
