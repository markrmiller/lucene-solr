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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;

import org.apache.solr.common.EmptyEntityResolver;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.XMLErrorLogger;
import org.codehaus.staxmate.dom.DOMConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.aalto.sax.SAXParserFactoryImpl;
import com.fasterxml.aalto.stax.InputFactoryImpl;

public class XMLFactorysAndParser {
  public static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  public static final XMLInputFactory inputFactory = new InputFactoryImpl();
  public static final SAXParserFactory saxFactory = new SAXParserFactoryImpl();
  public static final SAXParser docSaxParser;
  public static final SAXParser configSaxParser;
  public static final DOMConverter convertor = new DOMConverter();
  static {
    EmptyEntityResolver.configureXMLInputFactory(inputFactory);
    inputFactory.setXMLReporter(xmllog);
    saxFactory.setNamespaceAware(true); // XSL needs this!
    EmptyEntityResolver.configureSAXParserFactory(saxFactory);

    try {
      docSaxParser = saxFactory.newSAXParser();
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
    
    try {
      configSaxParser = saxFactory.newSAXParser();
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
  }
}
