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
package org.apache.solr;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CorePropertiesLocator;
import org.apache.solr.core.CoresLocator;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginBag;
import org.apache.solr.request.SolrRequestHandler;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SolrModule extends AbstractModule {
  
  private final NodeConfig nodeConfig;
  private final CorePropertiesLocator corePropertiesLocator;
  private final Properties extraProperties; // nocommit
  private final SolrZkClient zkClient;

  public SolrModule(SolrZkClient zkClient, NodeConfig nodeConfig, Properties extraProperties) {
    this.zkClient = zkClient;
    this.nodeConfig = nodeConfig;
    this.corePropertiesLocator = new CorePropertiesLocator(nodeConfig.getCoreRootDirectory());
    this.extraProperties = extraProperties;
  }
  
  @Override 
  protected void configure() {

    
    
    bind(Map.class).to(HashMap.class);
  }
  
  @Provides
  SolrZkClient provideSolrZkClient() {
   return  zkClient;
  }
  
  @Provides
  NodeConfig provideNodeConfig() {
   return  nodeConfig;
  }
  
  @Provides
  CoresLocator corePropertiesLocator() {
   return  corePropertiesLocator;
  }
  
  @Provides
  PluginBag<SolrRequestHandler> provideTransactionLog() {
   return  new PluginBag<>(SolrRequestHandler.class, null);
  }
}
