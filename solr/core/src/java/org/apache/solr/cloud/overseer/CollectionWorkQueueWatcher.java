package org.apache.solr.cloud.overseer;

import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.cloud.DistributedMap;
import org.apache.solr.cloud.DoNotWrap;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerConfigSetMessageHandler;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class CollectionWorkQueueWatcher extends QueueWatcher implements DoNotWrap {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler collMessageHandler;
  private final OverseerConfigSetMessageHandler configMessageHandler;
  private final DistributedMap failureMap;
  private final DistributedMap runningMap;

  private final DistributedMap completedMap;

  private volatile boolean checkAgain = false;
  private volatile boolean running;

  public CollectionWorkQueueWatcher(CoreContainer cc, String myId, LBHttp2SolrClient overseerExtLbClient, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer)
      throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_COLLECTION_QUEUE_WORK);
    collMessageHandler = new OverseerCollectionMessageHandler(cc, myId, overseerLbClient, overseerExtLbClient, adminPath, stats, overseer);
    configMessageHandler = new OverseerConfigSetMessageHandler(cc);
    failureMap = Overseer.getFailureMap(cc.getZkController().getZkClient());
    runningMap = Overseer.getRunningMap(cc.getZkController().getZkClient());
    completedMap = Overseer.getCompletedMap(cc.getZkController().getZkClient());
  }

  @Override
  public void close() {
    super.close();
    IOUtils.closeQuietly(collMessageHandler);
    IOUtils.closeQuietly(configMessageHandler);
  }

  @Override
  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;
    ourLock.lock();
    try {
      zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);

      startItems = getItems();

      log.info("Overseer found entries on start {}", startItems);
      if (startItems.size() > 0) {
        processQueueItems(startItems, true, weAreReplacement);
      }
    } finally {
      ourLock.unlock();
    }
  }

  protected List<String> getItems() {
    try {

      if (log.isDebugEnabled()) log.debug("get items from Overseer work queue {}", path);

      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);

      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      return items.subList(0, Math.min(100, items.size()));
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper session expired");
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Unexpected error in Overseer state update loop", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (!Event.EventType.NodeChildrenChanged.equals(event.getType())) {
      return;
    }
    if (this.closed || zkController.getZkClient().isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      return;
    }
    ourLock.lock();
    try {
      if (running) {
        checkAgain = true;
      } else {
        running = true;
        overseer.getTaskExecutor().submit(() -> {
          try {
            do {

              List<String> items = getItems();
              try {

                if (items.size() > 0) {
                  processQueueItems(items, false, false);
                }
              } catch (AlreadyClosedException e) {

              } catch (Exception e) {
                log.error("Exception during overseer queue queue processing", e);
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

  @Override
  protected void processQueueItems(List<String> items, boolean onStart, boolean weAreReplacement) {
    if (closed) return;

    List<String> fullPaths = new ArrayList<>(items.size());

    log.info("Found collection queue items {} onStart={}", items, onStart);
    for (String item : items) {
      fullPaths.add(path + "/" + item);
    }

    CountDownLatch deleteLatch = new CountDownLatch(fullPaths.size());
    for (String path : fullPaths) {
      try {
        zkController.getZkClient().getConnectionManager().getKeeper().getData(path, false, (rc, path1, ctx, data, stat) -> {

          if (rc != 0) {
            try {
              final KeeperException.Code keCode = KeeperException.Code.get(rc);
              if (keCode == KeeperException.Code.NONODE) {
                if (log.isDebugEnabled()) log.debug("No node found for {}", path1);
              }
            } finally {
              deleteLatch.countDown();
            }
          } else {
            try {
              zkController.getZkClient().deleteAsync(path1, -1, (rc1, path2, ctx2) -> {
                try {
                  if (rc != 0) {
                    log.error("got zk error deleting path {} {}", path1, rc);
                    KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
                    log.error("Exception deleting znode path=" + path1, e);
                  }
                } finally {
                  deleteLatch.countDown();
                }
              });
            } catch (Exception e) {
              log.error("Exception deleting queue item", e);
            }

            overseer.getTaskExecutor().submit(() -> {
              MDCLoggingContext.setNode(zkController.getNodeName());
              try {
                processEntry(path1, data, onStart);
              } catch (Exception e) {
                log.error("failed processing collection queue items " + items, e);
              }

            });
          }

        }, null);
      } catch (Exception e) {
        log.error("Exception getting queue data", e);
        deleteLatch.countDown();
      }
    }

    try {
      deleteLatch.await();
    } catch (InterruptedException e) {
      log.error("Interrupted", e);
    }

  }

  @Override
  protected void doneWithCurrentQueue() {

  }

  private void processEntry(String path, byte[] data, boolean onStart) {
    ZkStateWriter zkWriter = overseer.getZkStateWriter();
    if (zkWriter == null) {
      log.warn("Overseer appears closed");
      throw new AlreadyClosedException();
    }

    try {

      if (data == null) {
        log.error("empty item {}", path);
        return;
      }

      String responsePath = Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + OverseerTaskQueue.RESPONSE_PREFIX + path.substring(path.lastIndexOf("-") + 1);

      final ZkNodeProps message = ZkNodeProps.load(data);
      try {
        String operation = message.getStr(Overseer.QUEUE_OPERATION);

        if (operation == null) {
          log.error("Msg does not have required " + Overseer.QUEUE_OPERATION + ": {}", message);
          return;
        }

        final String asyncId = message.getStr(ASYNC);

        OverseerSolrResponse response;
        if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
          response = configMessageHandler.processMessage(message, operation, zkWriter);
        } else {
          response = collMessageHandler.processMessage(message, operation, zkWriter);
        }

        if (log.isDebugEnabled()) log.debug("response {}", response);

        if (response == null) {
          NamedList nl = new NamedList();
          nl.add("success", "true");
          response = new OverseerSolrResponse(nl);
        } else if (response.getResponse().size() == 0) {
          response.getResponse().add("success", "true");
        }

        if (asyncId != null) {

          if (log.isDebugEnabled()) {
            log.debug("Updated completed map for task with zkid:[{}]", asyncId);
          }
          completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response), CreateMode.PERSISTENT);

        } else {
          byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
          completedMap.update(path.substring(path.lastIndexOf("-") + 1), sdata);
          log.debug("Completed task:[{}] {} {}", message, response.getResponse(), responsePath);
        }

      } catch (Exception e) {
        log.error("Exception processing entry");
      }

    } catch (Exception e) {
      log.error("Exception processing entry", e);
    }

  }
}
