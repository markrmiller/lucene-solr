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
package org.apache.solr.handler.sql;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * JDBC driver for Calcite Solr.
 *
 * <p>It accepts connect strings that start with "jdbc:calcitesolr:".</p>
 */
public class CalciteSolrDriver extends Driver {
  public final static String CONNECT_STRING_PREFIX = "jdbc:calcitesolr:";

  private volatile static CloudSolrClient cloudSolrClient;
  
  private CalciteSolrDriver() {
    super();
  }

  static {
    new CalciteSolrDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if(!this.acceptsURL(url)) {
      return null;
    }

    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();

    String schemaName = info.getProperty("zk");
    if(schemaName == null) {
      throw new SQLException("zk must be set");
    }
    String zk = info.getProperty("zk");
    cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zk), Optional.empty())
        .withSocketTimeout(Integer.parseInt(System.getProperty("solr.socketTimeout", "90000")))
        .withConnectionTimeout(Integer.parseInt(System.getProperty("solr.connectTimeout", "30000"))).build();

    rootSchema.add(schemaName, new SolrSchema(info, cloudSolrClient));

    // Set the default schema
    calciteConnection.setSchema(schemaName);

    return connection;
  }

}
