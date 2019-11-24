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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pontusvision.nifi.nlp.PontusNLPProcessor.INDEX_URI;

public class PontusLuceneIndexWriterProcessorTest
{

  private TestRunner testRunnerWriter;
  private TestRunner testRunnerReader;

  public static final String ATTRIBUTE_INPUT_NAME = "sentence";

  @Before public void init()
  {
    testRunnerWriter = TestRunners.newTestRunner(PontusLuceneIndexWriterProcessor.class);
    testRunnerReader = TestRunners.newTestRunner(PontusLuceneIndexReaderProcessor.class);
  }

  @Test public void testWriterProcessorNames()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");

    testRunnerWriter.setProperty(INDEX_URI, "file:///tmp/Person.Identity.Last_Name");
    testRunnerReader.setProperty(INDEX_URI, "file:///tmp/Person.Identity.Last_Name");

    //    testRunner.setProperty(DICTIONARY_MODEL_JSON,DICTIONARY_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(SENTENCE_MODEL_JSON,SENTENCE_MODEL_JSON_DEFAULT_VAL);
    try
    {
      HashMap<String, String> attribs = new HashMap<>();
      testRunnerWriter.enqueue(new FileInputStream(new File("src/test/resources/en-dict-names.txt")), attribs);
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

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        testRunnerReader.enqueue(
            "[\"Leonardo\", \"Renata\", \"Edward\", \"John\", \"Joe\", \"banana\", \"queijo\", \"cheese\", \"cadeira\", \"453452\"]");
        testRunnerReader.run();
        List<MockFlowFile>  readerFiles = testRunnerReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);


        for (MockFlowFile readerFile: readerFiles)
        {
          Map<String, String> attributes = readerFile.getAttributes();

          assert (attributes.get("PERCENTAGE_MATCH").equals("70.0"));

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

  @Test public void testWriterProcessorNamesWithCitiesBeingSearched()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");

    testRunnerWriter.setProperty(INDEX_URI, "file:///tmp/Person.Identity.Last_Name");
    testRunnerReader.setProperty(INDEX_URI, "file:///tmp/Person.Identity.Last_Name");

    //    testRunner.setProperty(DICTIONARY_MODEL_JSON,DICTIONARY_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(SENTENCE_MODEL_JSON,SENTENCE_MODEL_JSON_DEFAULT_VAL);
    try
    {
      HashMap<String, String> attribs = new HashMap<>();
      testRunnerWriter.enqueue(new FileInputStream(new File("src/test/resources/en-dict-names.txt")), attribs);
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

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        testRunnerReader.enqueue(

            "[\"Leonardo\", \"Rio de Janeiro\", \"Petropolis\", \"Teresopolis\", \"Cubatao\", \"Botucatu\", \"Divinolândia\", \"Dobrada\", \"Cruzália\", \"Dracena\"]");
        testRunnerReader.run();
        List<MockFlowFile>  readerFiles = testRunnerReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);


        for (MockFlowFile readerFile: readerFiles)
        {
          Map<String, String> attributes = readerFile.getAttributes();

          assert (attributes.get("PERCENTAGE_MATCH").equals("30.0"));

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

  @Test public void testWriterProcessorNamesWithinText()
  {

    testRunnerWriter.setProperty(INDEX_URI, "file:///tmp/Location.Address.City");
    testRunnerReader.setProperty(INDEX_URI, "file:///tmp/Location.Address.City");

    try
    {
      HashMap<String, String> attribs = new HashMap<>();
      testRunnerWriter.enqueue(new FileInputStream(new File("src/test/resources/en-dict-names.txt")), attribs);
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

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        testRunnerReader.enqueue(
            "[\"Leonardo nasceu em  Cubatao, e se mudou para a China em Shanghai com o seu ratinho Toledo\","
                + " \"Voltaremos de aviao amanha.\", \"Petropolis\", \"Teresopolis\", \"Cubatao\", \"Botucatu\", \"queijo\", \"cheese\", \"cadeira\", \"453452\"]");
        testRunnerReader.run();
        List<MockFlowFile>  readerFiles = testRunnerReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);


        for (MockFlowFile readerFile: readerFiles)
        {
          Map<String, String> attributes = readerFile.getAttributes();

          assert (attributes.get("PERCENTAGE_MATCH").equals("50.0"));

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

  @Test public void testWriterProcessorCities()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");

    testRunnerWriter.setProperty(INDEX_URI, "file:///tmp/Location.Address.City");
    testRunnerReader.setProperty(INDEX_URI, "file:///tmp/Location.Address.City");

    //    testRunner.setProperty(DICTIONARY_MODEL_JSON,DICTIONARY_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(SENTENCE_MODEL_JSON,SENTENCE_MODEL_JSON_DEFAULT_VAL);
    try
    {
      HashMap<String, String> attribs = new HashMap<>();
      testRunnerWriter.enqueue(new FileInputStream(new File("src/test/resources/pt-municipios-names.txt")), attribs);
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

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        testRunnerReader.enqueue(
            "[\"Leonardo\", \"Rio de Janeiro\", \"Petropolis\", \"Teresopolis\", \"Cubatao\", \"Botucatu\", \"queijo\", \"cheese\", \"cadeira\", \"453452\"]");
        testRunnerReader.run();
        List<MockFlowFile>  readerFiles = testRunnerReader
            .getFlowFilesForRelationship(PontusLuceneIndexReaderProcessor.REL_SUCCESS);


        for (MockFlowFile readerFile: readerFiles)
        {
          Map<String, String> attributes = readerFile.getAttributes();

          assert (attributes.get("PERCENTAGE_MATCH").equals("50.0"));

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
  //  @Test public void testProcessor()
  //  {
  //
  //    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
  //    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");
  //
  //    testRunnerWriter.setProperty(TOKENIZER_MODEL_JSON,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
  //    testRunnerReader.setProperty(TOKENIZER_MODEL_JSON,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
  //
  //
  //    //    testRunner.setProperty(DICTIONARY_MODEL_JSON,DICTIONARY_MODEL_JSON_DEFAULT_VAL);
  //    //    testRunner.setProperty(SENTENCE_MODEL_JSON,SENTENCE_MODEL_JSON_DEFAULT_VAL);
  //    try
  //    {
  //      HashMap<String,String> attribs = new HashMap<>();
  //      // add a fake entry to see whether it gets preserved.
  //
  //      attribs.put("pg_nlp_res_twitterhandle","[\"WEIRD_VALUE_GOES_HERE\"]");
  //
  //      testRunnerWriter.enqueue(new FileInputStream(new File("src/test/resources/large.txt")),attribs);
  //    }
  //    catch (FileNotFoundException e)
  //    {
  //      e.printStackTrace();
  //    }
  //
  //    testRunnerWriter.setValidateExpressionUsage(true);
  //    testRunnerWriter.run();
  //    testRunnerWriter.assertValid();
  //    List<MockFlowFile> successFiles = testRunnerWriter.getFlowFilesForRelationship(PontusNLPProcessor.REL_SUCCESS);
  //
  //    for (MockFlowFile mockFile : successFiles)
  //    {
  //      try
  //      {
  //        Map<String, String> attributes = mockFile.getAttributes();
  //
  //        for (String attribute : attributes.keySet())
  //        {
  //          System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
  //        }
  //
  //      }
  //      catch (Throwable e)
  //      {
  //        e.printStackTrace();
  //      }
  //    }
  //  }

}
