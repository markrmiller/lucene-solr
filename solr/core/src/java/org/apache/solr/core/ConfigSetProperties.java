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
package org.apache.solr.core;

import static org.apache.solr.common.util.Utils.fromJSON;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class ConfigSetProperties {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DEFAULT_FILENAME = "configsetprops.json";
  public static final String IMMUTABLE_CONFIGSET_ARG = "immutable";

  /**
   * Return the properties associated with the ConfigSet (e.g. immutable)
   *
   * @param loader
   *          the resource loader
   * @param name
   *          the name of the config set properties file
   * @return the properties in a NamedList
   */
  public static NamedList readFromResourceLoader(SolrResourceLoader loader, String name) {
    try (InputStream is = loader.openResource(name)) {
      try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
        return readFromInputStream(reader);
      }
    } catch (SolrResourceNotFoundException ex) {
      log.info("Did not find ConfigSet properties, assuming default properties");
      return new NamedList();
    } catch (Exception ex) {
      throw new SW.Exp(ex);
    }

  }

  public static NamedList readFromInputStream(InputStreamReader reader) {
    try {
      Object object = fromJSON(reader);
      if (object == null) {
        return new NamedList();
      }
      if (!(object instanceof Map)) {
        final String objectClass = object == null ? "null" : object.getClass().getName();
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid JSON type " + objectClass + ", expected Map");
      }
      return new NamedList();
    } catch (Exception ex) {
      throw new SW.Exp("Did not find ConfigSet properties", ex);
    }
  }
}
