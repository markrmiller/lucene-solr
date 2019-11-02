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
package org.apache.solr.SolrResources;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.client.HttpClient;
import org.apache.solr.SolrModule;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.core.ZkContainer;
import org.apache.solr.metrics.AltBufferPoolMetricSet;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.OperatingSystemMetricSet;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class SolrResources implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean closed = false;
  private volatile boolean started = false;
  
  private final String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  private final Properties extraProperties;
  private final Path solrHomePath;

  private volatile Injector injector;

  private volatile SolrZkClient zkClient;
  private volatile HttpClient httpClient;
  private volatile SolrMetricManager metricManager;
  private volatile NodeConfig nodeConfig;
  private volatile CoreContainer cores;
  private volatile String registryName;
  
  private final AtomicBoolean loadLock = new AtomicBoolean();

  public SolrResources(Path solrHomePath, Properties extraProperties) {
    this.solrHomePath = solrHomePath;
    this.extraProperties = extraProperties;
  }

  public void start() {
    // nocommit error if start not called
    if (started) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "SolrResources already started");
    }
    
    if (closed) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Already closed");
    }

    started = true;

    loadLock.set(true);
    try {

      String zkHost = System.getProperty("zkHost");
      if (!StringUtils.isEmpty(zkHost)) {
        int startUpZkTimeOut = Integer.getInteger("waitForZk", 10);
        log.info("Using connectString={}", zkHost);
        // nocommit for now we are bridging to a transition
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkHost).retryPolicy(retryPolicy)
            .defaultData(null).build();
        client.start();
        try {
          client.blockUntilConnected(startUpZkTimeOut, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          DW.propegateInterrupt(e);
        }

        zkClient = new SolrZkClient(client);
      }
      nodeConfig = loadNodeConfig(solrHomePath, extraProperties, zkClient);

      // final CoreContainer coreContainer = new CoreContainer(nodeConfig, extraProperties,
      // new CorePropertiesLocator(nodeConfig.getCoreRootDirectory()), zkClient);

      SolrModule solrModule = new SolrModule(zkClient, nodeConfig, extraProperties);
      injector = Guice.createInjector(solrModule);

      cores = injector.getInstance(CoreContainer.class);

      cores.load(true);
      SolrResourceLoader.ensureUserFilesDataDir(solrHomePath);
      this.httpClient = cores.getUpdateShardHandler().getDefaultHttpClient();
      registryName = setupJvmMetrics(cores);
    } finally {
      loadLock.set(false);
      synchronized (loadLock) {
        loadLock.notifyAll();
      }
    }
  }

  @Override
  public void close() throws IOException {
    log.info("Close SolrResources");
    if (closed) {
      throw new AlreadyClosedException();
    }
    closed = true;
    
    
    while (loadLock.get()) {
      synchronized (loadLock) {
        try {
          loadLock.wait();
        } catch (InterruptedException e) {
          DW.propegateInterrupt(e);
        }
      }
    }
    
    // nocommit
    try (DW worker = new DW(this, true)) {
      worker.add("SolrResourcesInternal",

          () -> {
            SolrMetricManager mm = metricManager;
            if (mm != null) {
              try {
                mm.unregisterGauges(registryName, metricTag);
              } catch (NullPointerException e) {
                // okay
              } catch (Exception e) {
                log.warn("Exception closing FileCleaningTracker", e);
              } finally {
                metricManager = null;
              }

            }
            return "MetricManager";
          });

      worker.add("CoreContainer", cores);
      worker.add(zkClient);
    }
  }

  private String setupJvmMetrics(CoreContainer coresInit) {
    metricManager = coresInit.getMetricManager();
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm);
    final Set<String> hiddenSysProps = coresInit.getConfig().getMetricsConfig().getHiddenSysProps();
    try {
      metricManager.registerAll(registryName, new AltBufferPoolMetricSet(), true, "buffers");
      metricManager.registerAll(registryName, new ClassLoadingGaugeSet(), true, "classes");
      metricManager.registerAll(registryName, new OperatingSystemMetricSet(), true, "os");
      metricManager.registerAll(registryName, new GarbageCollectorMetricSet(), true, "gc");
      metricManager.registerAll(registryName, new MemoryUsageGaugeSet(), true, "memory");
      metricManager.registerAll(registryName, new ThreadStatesGaugeSet(), true, "threads"); // todo should we use
                                                                                            // CachedThreadStatesGaugeSet
                                                                                            // instead?
      MetricsMap sysprops = new MetricsMap((detailed, map) -> {
        System.getProperties().forEach((k, v) -> {
          if (!hiddenSysProps.contains(k)) {
            map.put(String.valueOf(k), v);
          }
        });
      });
      metricManager.registerGauge(null, registryName, sysprops, metricTag, true, "properties", "system");
      return registryName;
    } catch (Exception e) {
      throw new DW.Exp("Error registering JVM metrics", e);
    }
  }

  /**
   * Get the NodeConfig whether stored on disk, in ZooKeeper, etc. This may also be used by custom filters to load
   * relevant configuration.
   * 
   * @return the NodeConfig
   */
  public static NodeConfig loadNodeConfig(Path solrHome, Properties nodeProperties, SolrZkClient zkClient) {
    NodeConfig cfg = null;
    try (SolrResourceLoader loader = new SolrResourceLoader(solrHome, SolrDispatchFilter.class.getClassLoader(),
        nodeProperties)) {
      if (!StringUtils.isEmpty(System.getProperty("solr.solrxml.location"))) {
        log.warn("Solr property solr.solrxml.location is no longer supported. " +
            "Will automatically load solr.xml from ZooKeeper if it exists");
      }

      if (zkClient != null) {
        // zkClient.printLayout(); // nocommit

        try {
          if (zkClient.exists("/solr.xml", true)) {
            log.info("solr.xml found in ZooKeeper. Loading...");
            byte[] data = zkClient.getData("/solr.xml", null, null, true);
            return SolrXmlConfig.fromInputStream(loader, new ByteArrayInputStream(data));
          }
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Error occurred while loading solr.xml from zookeeper", e);
        }
        log.info("Loading solr.xml from SolrHome (not found in ZooKeeper)");
      }
      cfg = SolrXmlConfig.fromSolrHome(loader, loader.getInstancePath());
    } catch (IOException e) {
      // do nothing.
    }
    return cfg;
  }

  public CoreContainer getCoreContainer() {
    return cores;
  }

  public static void waitForClose() {
    // nocommit

  }
}
