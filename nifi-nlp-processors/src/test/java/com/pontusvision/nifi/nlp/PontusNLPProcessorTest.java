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

import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
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

import static com.pontusvision.nifi.nlp.PontusNLPProcessor.*;

public class PontusNLPProcessorTest
{
  protected ModelJSONValidator<TokenizerModel> tokenizerModelModelJSONValidator = new ModelJSONValidator<>(
      TokenizerModel.class);
  protected ModelJSONValidator<TokenNameFinderModel> tokenNameFinderModelModelJSONValidator = new ModelJSONValidator<>(
      TokenNameFinderModel.class);

  // public static final String ATTRIBUTE_OUTPUT_NAME = "names";
  // public static final String ATTRIBUTE_OUTPUT_LOCATION_NAME = "locations";
  // public static final String ATTRIBUTE_OUTPUT_DATE_NAME = "dates";
//  public static final String TOKENIZER_MODEL_JSON = "Tokenizer Model in JSON";
//
//  public static final String TOKENIZER_MODEL_JSON_DEFAULT_VAL =
//      "{\n" + "  \"englishTokens\": \"http://opennlp.sourceforge.net/models-1.5/en-token.bin\"\n"
//          + "}";
//
//  public final PropertyDescriptor TOKENIZER_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
//      .name(TOKENIZER_MODEL_JSON).description(
//          "A JSON File with the data type to be received, and a URL pointing to the files (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin")
//      .addValidator(tokenizerModelModelJSONValidator).expressionLanguageSupported(true).required(true)
//      .defaultValue(TOKENIZER_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();
//
//  public static final String TOKEN_NAME_FINDER_MODEL_JSON = "Token Name Finder Model in JSON";
//
//  public static final String TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL =
//      "{\n" + "  \"person\": \"https://github.com/armenak/DataDefender/blob/master/sample_projects/file_discovery/en-ner-person.bin\"\n"
//          + " ,\"location\": \"http://opennlp.sourceforge.net/models-1.5/en-ner-location.bin\"\n"
//          + " ,\"date\": \"http://opennlp.sourceforge.net/models-1.5/en-ner-date.bin\"\n"
//          + " ,\"money\": \"http://opennlp.sourceforge.net/models-1.5/en-ner-money.bin\"\n"
//          + " ,\"organization\": \"http://opennlp.sourceforge.net/models-1.5/en-ner-organization.bin\"\n"
//          + " ,\"time\": \"http://opennlp.sourceforge.net/models-1.5/en-ner-time.bin\"\n"
//          + "}";
//
//  public final PropertyDescriptor TOKEN_NAME_FINDER_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
//      .name(TOKEN_NAME_FINDER_MODEL_JSON).description(
//          "A JSON Object with the data types to be found, and a URL pointing to the model files (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin)")
//      .addValidator(tokenNameFinderModelModelJSONValidator).expressionLanguageSupported(true).required(true)
//      .defaultValue(TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();



  private TestRunner testRunner;

  public static final String ATTRIBUTE_INPUT_NAME = "sentence";

  @Before public void init()
  {
    testRunner = TestRunners.newTestRunner(PontusNLPProcessor.class);
  }

  @Test public void testProcessor()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");

    testRunner.setProperty(TOKENIZER_MODEL_JSON,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    testRunner.setProperty(TOKEN_NAME_FINDER_MODEL_JSON,TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);
//    testRunner.setProperty(DICTIONARY_MODEL_JSON,DICTIONARY_MODEL_JSON_DEFAULT_VAL);
//    testRunner.setProperty(SENTENCE_MODEL_JSON,SENTENCE_MODEL_JSON_DEFAULT_VAL);
    try
    {
      HashMap<String,String> attribs = new HashMap<>();
      // add a fake entry to see whether it gets preserved.

      attribs.put("pg_nlp_res_twitterhandle","[\"WEIRD_VALUE_GOES_HERE\"]");

      testRunner.enqueue(new FileInputStream(new File("src/test/resources/large.txt")),attribs);
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
