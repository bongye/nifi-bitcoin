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
package com.kisline.processors.bitcoin;

import com.kisline.dbcp.DBCPConfigUtil;
import com.kisline.dbcp.HikariCPService;
import com.kisline.dbcp.StandardHikariCPService;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BitcoinHistoryProcessorTest {
  private static final String TEST_FILE = "test.csv";
  private static final String BAD_TEST_FILE = "bad.csv";
  private static NetworkServerControl derby;

  private TestRunner testRunner;
  private Path input;
  private Path badInput;

  @BeforeClass
  public static void start() throws Exception {
    derby = new NetworkServerControl();
    derby.start(new PrintWriter(System.out));
  }

  @AfterClass
  public static void shutdown() throws Exception {
    derby.shutdown();
  }

  @Before
  public void init() throws Exception {
    testRunner = TestRunners.newTestRunner(BitcoinHistoryProcessor.class);
    final HikariCPService service = new StandardHikariCPService();
    testRunner.addControllerService("dbcp", service);

    testRunner.setProperty(
        service, DBCPConfigUtil.DATASOURCE_CLASSNAME, ClientDataSource.class.getName());
    testRunner.setProperty(service, DBCPConfigUtil.USERNAME, "test");
    testRunner.setProperty(service, DBCPConfigUtil.PASSWORD, "test");
    testRunner.setProperty(service, DBCPConfigUtil.AUTO_COMMIT, "true");
    testRunner.setProperty(service, DBCPConfigUtil.METRICS, "true");

    testRunner.setProperty(service, "databaseName", "test");
    testRunner.setProperty(service, "createDatabase", "create");
    testRunner.setProperty(service, "serverName", "localhost");
    testRunner.setProperty(service, "portNumber", "1527");

    testRunner.enableControllerService(service);
    testRunner.setProperty(ConfigUtil.DS_PROP, "dbcp");

    input = Paths.get(ClassLoader.getSystemResource(TEST_FILE).toURI());
    badInput = Paths.get(ClassLoader.getSystemResource(BAD_TEST_FILE).toURI());
  }

  @Test
  public void testAllOuptut() throws Exception {
    testRunner.enqueue(input);
    testRunner.setClustered(true);
    testRunner.setProperty(ConfigUtil.OUTPUT, "ALL");

    testRunner.run();

    testRunner.assertQueueEmpty();

    testRunner.assertTransferCount(ConfigUtil.JSON, 1);
    testRunner.assertTransferCount(ConfigUtil.XML, 1);
    testRunner.assertTransferCount(ConfigUtil.FAILURE, 0);

    testRunner.shutdown();
  }

  @Test
  public void testJsonOnlyOuptut() throws Exception {
    testRunner.enqueue(input);
    testRunner.setClustered(true);
    testRunner.setProperty(ConfigUtil.OUTPUT, "JSON");

    testRunner.run();

    testRunner.assertQueueEmpty();

    testRunner.assertTransferCount(ConfigUtil.JSON, 1);
    testRunner.assertTransferCount(ConfigUtil.XML, 0);
    testRunner.assertTransferCount(ConfigUtil.FAILURE, 0);

    testRunner.shutdown();
  }

  @Test
  public void testXmlOnlyOuptut() throws Exception {
    testRunner.enqueue(input);
    testRunner.setClustered(true);
    testRunner.setProperty(ConfigUtil.OUTPUT, "XML");

    testRunner.run();

    testRunner.assertQueueEmpty();

    testRunner.assertTransferCount(ConfigUtil.JSON, 0);
    testRunner.assertTransferCount(ConfigUtil.XML, 1);
    testRunner.assertTransferCount(ConfigUtil.FAILURE, 0);

    testRunner.shutdown();
  }

  @Test
  public void testBadOuptut() throws Exception {
    testRunner.enqueue(badInput);
    testRunner.setClustered(true);
    testRunner.setProperty(ConfigUtil.OUTPUT, "ALL");

    testRunner.run();

    testRunner.assertQueueEmpty();

    testRunner.assertTransferCount(ConfigUtil.JSON, 0);
    testRunner.assertTransferCount(ConfigUtil.XML, 0);
    testRunner.assertTransferCount(ConfigUtil.FAILURE, 1);

    testRunner.shutdown();
  }
}
