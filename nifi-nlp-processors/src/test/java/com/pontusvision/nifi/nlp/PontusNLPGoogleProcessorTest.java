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
import java.util.List;
import java.util.Map;

public class PontusNLPGoogleProcessorTest
{

  private TestRunner testRunner;



  @Before public void init()
  {
    testRunner = TestRunners.newTestRunner(PontusNLPGoogleProcessor.class);

  }

  @Test public void testProcessor()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
//    testRunner.setProperty(PontusNLPWatsonProcessor.AWS_ACCESS_KEY_PROP, user);
//    testRunner.setProperty(PontusNLPWatsonProcessor.AWS_SECRET_PROP, password);

    //    testRunner.setProperty(TOKENIZER_MODEL_JSON_PROP,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(TOKEN_NAME_FINDER_MODEL_JSON_PROP,TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);
    try
    {
      testRunner.enqueue(new FileInputStream(new File("src/test/resources/large.txt")));
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }

    testRunner.setValidateExpressionUsage(false);
    testRunner.run();
    testRunner.assertValid();
    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PontusNLPProcessor.REL_SUCCESS);

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        Map<String, String> attributes = mockFile.getAttributes();

        for (String attribute : attributes.keySet())
        {
          System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
        }

      }
      catch (Throwable e)
      {
        e.printStackTrace();
      }
    }
  }

  @Test public void testProcessorRealTweet()
  {

    testRunner.setProperty(PontusNLPProcessor.DATA_TO_PARSE,
        "RT @mclynd: Elon Musk and Bill Gates talk about #ArtificialIntelligence future possibilities https://t.co/0iIT4uepdw #DataScience #DataScie… Tuesday April 5, 2017 in New York.");
    //    testRunner.setProperty(TOKENIZER_MODEL_JSON_PROP,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(TOKEN_NAME_FINDER_MODEL_JSON_PROP,TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);

    try
    {
      testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.csv")));
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }

    testRunner.setValidateExpressionUsage(true);
    testRunner.run();
    testRunner.assertValid();
    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PontusNLPProcessor.REL_SUCCESS);

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        Map<String, String> attributes = mockFile.getAttributes();

        for (String attribute : attributes.keySet())
        {
          System.out.println("Sentence Tweet Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
        }
      }
      catch (Throwable e)
      {
        e.printStackTrace();
      }
    }
  }

}
