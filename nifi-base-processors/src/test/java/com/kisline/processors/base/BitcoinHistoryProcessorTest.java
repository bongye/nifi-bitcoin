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
package com.kisline.processors.base;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BitcoinHistoryProcessorTest {
  private static final String TEST_FILE = "test.csv";
  private static final String BAD_TEST_FILE = "bad.csv";

  private TestRunner testRunner;
  private Path input;
  private Path badInput;

  @Before
  public void init() throws Exception {
    testRunner = TestRunners.newTestRunner(BitcoinHistoryProcessor.class);
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
