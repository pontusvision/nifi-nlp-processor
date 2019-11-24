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
package com.pontusvision.nifi.nlp;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.pontusvision.nifi.nlp.PontusProcessorBase.DOMAIN;
import static com.pontusvision.nifi.nlp.PontusProcessorBase.REGEX_PATTERN;

public class PontusDiscoveryRegexDBClassifierProcessorTest extends PontusDiscoveryDBClassifierProcessorTest
{

  protected TestRunner testDiscoveryDBClassifierEmailsReader;
  protected TestRunner testDiscoveryDBClassifierCPFReader;

  @Before public void init()
  {

    super.init();
    testDiscoveryDBClassifierEmailsReader = TestRunners.newTestRunner(PontusDiscoveryRegexDBClassifierProcessor.class);
    testDiscoveryDBClassifierEmailsReader.setProperty(DOMAIN,"Object.Email.Address");

    testDiscoveryDBClassifierEmailsReader.setProperty(REGEX_PATTERN,
        "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])");

    testDiscoveryDBClassifierCPFReader = TestRunners.newTestRunner(PontusDiscoveryRegexDBClassifierProcessor.class);
    testDiscoveryDBClassifierCPFReader.setProperty(REGEX_PATTERN, "^([-\\.\\s]?(\\d{3})){3}[-\\.\\s]?(\\d{2})$");
    testDiscoveryDBClassifierCPFReader.setProperty(DOMAIN,"Person.Identity.ID");

    //    testDiscoveryDBClassifierCitiesReader = TestRunners.newTestRunner(PontusDiscoveryDBClassifierProcessor.class);

  }

  @Test public void testWriterProcessorNames()
  {

    try
    {

      String[] fileNames =
          {
              //              "src/test/resources/col_metadata_table1.json",
              //              "src/test/resources/col_metadata_table2.json",
              //              "src/test/resources/col_metadata_table3.json",
              "src/test/resources/col_metadata_table4.json"
              //              ,
              //              "src/test/resources/col_metadata_table5.json",
              //              "src/test/resources/col_metadata_table6.json"
             ,"src/test/resources/col_metadata_table7.json"
          };
      List<MockFlowFile> readerFiles = new LinkedList<>();

      for (String fileName : fileNames)
      {
        Map<String, String> attribs = new HashMap<>();
        addAttribFromFile(attribs, "pg_rdb_col_metadata", fileName);

        testDiscoveryDBClassifierEmailsReader.enqueue("", attribs);
        testDiscoveryDBClassifierEmailsReader.run();

        List<MockFlowFile> rFs = testDiscoveryDBClassifierEmailsReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);
        testDiscoveryDBClassifierEmailsReader.clearTransferState();

        rFs = runIndexTestReader(testDiscoveryDBClassifierCPFReader, rFs);

        rFs = runIndexTestReader(testDiscoveryDBClassifierNamesReader, rFs);
        rFs = runIndexTestReader(testDiscoveryDBClassifierCitiesReader, rFs);
        rFs = runIndexTestReader(testDiscoveryDBClassifierGenderReader, rFs);


        readerFiles.addAll(rFs);

      }

      for (MockFlowFile readerFile : readerFiles)
      {

        Map<String, String> attributes = readerFile.getAttributes();

        for (String attribute : attributes.keySet())
        {
          System.out.println("Attribute:" + attribute + " = " + readerFile.getAttribute(attribute));
        }
      }

    }
    catch (Throwable e)
    {
      e.printStackTrace();
    }

  }

}
