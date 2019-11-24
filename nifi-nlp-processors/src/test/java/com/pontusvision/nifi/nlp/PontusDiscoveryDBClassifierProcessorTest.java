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

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.pontusvision.nifi.nlp.PontusNLPProcessor.INDEX_URI;

public class PontusDiscoveryDBClassifierProcessorTest
{

  protected TestRunner testRunnerNamesWriter;
  protected TestRunner testRunnerCitiesWriter;
  protected TestRunner testRunnerGenderWriter;
  protected TestRunner testDiscoveryDBClassifierNamesReader;
  protected TestRunner testDiscoveryDBClassifierCitiesReader;
  protected TestRunner testDiscoveryDBClassifierGenderReader;

  protected Map<String, String> addAttribFromFile(Map<String, String> attribs, String attribName, String fileName)
      throws IOException
  {
    String val = new String(Files.readAllBytes(Paths.get(fileName)));

    attribs.putAll(ImmutableMap.of(
        attribName, val));
    return attribs;
  }

  public static TestRunner prepareIndexWriter(String indexURI, String dictionaryFileName)
  {
    TestRunner testRunnerWriter = TestRunners.newTestRunner(PontusLuceneIndexWriterProcessor.class);
    testRunnerWriter.setProperty(INDEX_URI, indexURI);
    try
    {
      HashMap<String, String> attribs = new HashMap<>();
      testRunnerWriter.enqueue(new FileInputStream(new File(dictionaryFileName)), attribs);
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }

    testRunnerWriter.setValidateExpressionUsage(true);
    testRunnerWriter.run();
    testRunnerWriter.assertValid();
    List<MockFlowFile> successFiles = testRunnerWriter
        .getFlowFilesForRelationship(PontusLuceneIndexWriterProcessor.REL_SUCCESS);

    assert (successFiles.size() == 1);

    return testRunnerWriter;

  }

  public  static TestRunner prepareIndexReader (String indexFileName)
  {
    TestRunner retVal =  TestRunners.newTestRunner(PontusDiscoveryDBClassifierProcessor.class);

    retVal.setProperty(INDEX_URI, indexFileName);

    return retVal;
  }

  @Before public void init()
  {
    testRunnerNamesWriter = prepareIndexWriter("file:///tmp/Person.Identity.Last_Name",
        "src/test/resources/en-dict-names.txt");
    testRunnerCitiesWriter =  prepareIndexWriter( "file:///tmp/Location.Address.City",
        "src/test/resources/pt-municipios-names.txt");
    testRunnerGenderWriter = prepareIndexWriter( "file:///tmp/Person.Identity.Gender",
        "src/test/resources/gender-examples.txt");

    testDiscoveryDBClassifierNamesReader = prepareIndexReader( "file:///tmp/Person.Identity.Last_Name");

    testDiscoveryDBClassifierCitiesReader =  prepareIndexReader( "file:///tmp/Location.Address.City");

    testDiscoveryDBClassifierGenderReader = prepareIndexReader( "file:///tmp/Person.Identity.Gender");


  }

  public static List<MockFlowFile>   runIndexTestReader(TestRunner indexReader, List<MockFlowFile> rFs){
    indexReader.enqueue(rFs.toArray(new MockFlowFile[0]));
    indexReader.run();
    rFs = indexReader
        .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);

    indexReader.clearTransferState();

    return rFs;
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
          };
      List<MockFlowFile> readerFiles = new LinkedList<>();

      for (String fileName : fileNames)
      {

        Map<String, String> attribs = new HashMap<>();
        addAttribFromFile(attribs, "pg_rdb_col_metadata", fileName);

        testDiscoveryDBClassifierNamesReader.enqueue("", attribs);
        testDiscoveryDBClassifierNamesReader.run();




        List<MockFlowFile> rFs = testDiscoveryDBClassifierNamesReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);

        testDiscoveryDBClassifierNamesReader.clearTransferState();

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
