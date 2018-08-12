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

import com.google.gson.Gson;
import opennlp.tools.dictionary.Dictionary;
import opennlp.tools.namefind.DictionaryNameFinder;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.RegexNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Tags({
    "Pontus Vision, nlpprocessor, apache opennlp, nlp, natural language processing" }) @CapabilityDescription("Run OpenNLP Natural Language Processing for Name, Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") }) @WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }) public class PontusNLPProcessor
    extends AbstractProcessor
{

  //  protected ModelJSONValidator<DictionaryNameFinder>

  protected ModelJSONValidator<TokenizerModel> tokenizerModelModelJSONValidator = new ModelJSONValidator<>(
      TokenizerModel.class);
  protected ModelJSONValidator<SentenceModel> sentenceModelModelJSONValidator = new ModelJSONValidator<>(
      SentenceModel.class);
  protected ModelJSONValidator<TokenNameFinderModel> tokenNameFinderModelModelJSONValidator = new ModelJSONValidator<>(
      TokenNameFinderModel.class);

  protected DictionaryJSONValidator dictionaryJSONValidator = new DictionaryJSONValidator();

  protected RegexJSONValidator regexJSONValidator = new RegexJSONValidator();

  public static final String THRESHOLD_PROB = "Probability Threshold";
  public static final String THRESHOLD_PROB_DEFAULT_VAL = "-0.01";

  Double thresholdProb = Double.parseDouble(THRESHOLD_PROB_DEFAULT_VAL);

  public final PropertyDescriptor THRESHOLD_PROB_PROP = new PropertyDescriptor.Builder().name(THRESHOLD_PROB)
      .description(
          "The threshold (0.0 - 1.0) that a particular value was a match; any values lower than this will be discarded.")
      .addValidator(StandardValidators.NUMBER_VALIDATOR).expressionLanguageSupported(true).required(true)
      .defaultValue(THRESHOLD_PROB_DEFAULT_VAL).dynamic(true).build();

  public static final String REGEX_MODEL_JSON = "Regex Model in JSON";

  public static final String REGEX_MODEL_JSON_DEFAULT_VAL =
      "{"
      + " \"email\":     \"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\\\"(?:[\\\\x01-\\\\x08\\\\x0b\\\\x0c\\\\x0e-\\\\x1f\\\\x21\\\\x23-\\\\x5b\\\\x5d-\\\\x7f]|\\\\[\\\\x01-\\\\x09\\\\x0b\\\\x0c\\\\x0e-\\\\x7f])*\\\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\\\x01-\\\\x08\\\\x0b\\\\x0c\\\\x0e-\\\\x1f\\\\x21-\\\\x5a\\\\x53-\\\\x7f]|\\\\[\\\\x01-\\\\x09\\\\x0b\\\\x0c\\\\x0e-\\\\x7f])+)\\\\])\"\n"
      + ",\"URL\":       \"(?:(?:https?|ftp)://)(?:\\\\S+(?::\\\\S*)?@)?(?:(?!10(?:\\\\.\\\\d{1,3}){3})(?!127(?:\\\\.\\\\d{1,3}){3})(?!169\\\\.254(?:\\\\.\\\\d{1,3}){2})(?!192\\\\.168(?:\\\\.\\\\d{1,3}){2})(?!172\\\\.(?:1[6-9]|2\\\\d|3[0-1])(?:\\\\.\\\\d{1,3}){2})(?:[1-9]\\\\d?|1\\\\d\\\\d|2[01]\\\\d|22[0-3])(?:\\\\.(?:1?\\\\d{1,2}|2[0-4]\\\\d|25[0-5])){2}(?:\\\\.(?:[1-9]\\\\d?|1\\\\d\\\\d|2[0-4]\\\\d|25[0-4]))|(?:(?:[a-z\\\\x{00a1}-\\\\x{ffff}0-9]+-?)*[a-z\\\\x{00a1}-\\\\x{ffff}0-9]+)(?:\\\\.(?:[a-z\\\\x{00a1}-\\\\x{ffff}0-9]+-?)*[a-z\\\\x{00a1}-\\\\x{ffff}0-9]+)*(?:\\\\.(?:[a-z\\\\x{00a1}-\\\\x{ffff}]{2,})))(?::\\\\d{2,5})?(?:/[^\\\\s]*)?\"\n"
      + ",\"phone\":     \"(?:(?:\\\\+?([1-9]|[0-9][0-9]|[0-9][0-9][0-9])\\\\s*(?:[.-]\\\\s*)?)?(?:\\\\(\\\\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\\\\s*\\\\)|([0-9][1-9]|[0-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\\\\s*(?:[.-]\\\\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\\\\s*(?:[.-]\\\\s*)?([0-9]{4})(?:\\\\s*(?:#|x\\\\.?|ext\\\\.?|extension)\\\\s*(\\\\d+))?\"\n"
      + ",\"cred_card\": \"\\\\b(?:\\\\d[ -]*?){13,16}\\\\b\"\n"
      + ",\"twitterHandle\": \"\\\\@([a-z0-9_]{1,15})\"\n"
      + ",\"post_code\": \"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z]))))\\\\s?[0-9][A-Za-z]{2})\"\n"
      + "}";

  public final PropertyDescriptor REGEX_MODEL_JSON_PROP = new PropertyDescriptor.Builder().name(REGEX_MODEL_JSON)
      .description("A JSON Object with the data type to be processed, and a regex string")
      .addValidator(regexJSONValidator).expressionLanguageSupported(true).required(true)
      .defaultValue(REGEX_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();

  public static final String DICTIONARY_MODEL_JSON = "Dictionary Model in JSON";

  public static final String defaultPersonDictURLStr = PontusNLPProcessor.class.getResource("/en-dict-names.txt")
      .toString();

  public static final String DICTIONARY_MODEL_JSON_DEFAULT_VAL = "{\"person\": \"" + defaultPersonDictURLStr + "\"}";

  public final PropertyDescriptor DICTIONARY_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
      .name(DICTIONARY_MODEL_JSON).description(
          "A JSON Object with the data type to be processed, and a URL pointing to the dictionary (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin")
      .addValidator(dictionaryJSONValidator).expressionLanguageSupported(true).required(true)
      .defaultValue(DICTIONARY_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();




  public static final String SENTENCE_MODEL_JSON = "Sentence Model in JSON";
//  public static final String defaultenglishTokensURLStr = PontusNLPProcessor.class.getResource("/en-token.bin")
//      .toString();
    public static final String defaultenglishSentensesURLStr = PontusNLPProcessor.class.getResource("/en-sent.bin").toString();

  public static final String SENTENCE_MODEL_JSON_DEFAULT_VAL =
      "{\n" + "  \"englishSentenses\": \"" + defaultenglishSentensesURLStr + "\"\n"
          //          + " ,\"englishSentenses\": \""+defaultenglishSentensesURLStr+"\"\n"
          + "}";

  public final PropertyDescriptor SENTENCE_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
      .name(SENTENCE_MODEL_JSON).description(
          "A JSON Object with the data type to be received, and a URL pointing to the tokenizer model (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin")
      .addValidator(sentenceModelModelJSONValidator).expressionLanguageSupported(true).required(true)
      .defaultValue(SENTENCE_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();






  public static final String TOKENIZER_MODEL_JSON = "Tokenizer Model in JSON";
  public static final String defaultenglishTokensURLStr = PontusNLPProcessor.class.getResource("/en-token.bin")
      .toString();
  //  public static final String defaultenglishSentensesURLStr = PontusNLPProcessor.class.getResource("/en-sent.bin").toString();

  public static final String TOKENIZER_MODEL_JSON_DEFAULT_VAL =
      "{\n" + "  \"englishTokens\": \"" + defaultenglishTokensURLStr + "\"\n"
          //          + " ,\"englishSentenses\": \""+defaultenglishSentensesURLStr+"\"\n"
          + "}";

  public final PropertyDescriptor TOKENIZER_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
      .name(TOKENIZER_MODEL_JSON).description(
          "A JSON Object with the data type to be received, and a URL pointing to the tokenizer model (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin")
      .addValidator(tokenizerModelModelJSONValidator).expressionLanguageSupported(true).required(true)
      .defaultValue(TOKENIZER_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();

  public static final String TOKEN_NAME_FINDER_MODEL_JSON = "Token Name Finder Model in JSON";
  public static final String defaultPersonNerURLStr = PontusNLPProcessor.class.getResource("/en-ner-person.bin")
      .toString();
  public static final String defaultLocationNerURLStr = PontusNLPProcessor.class.getResource("/en-ner-location.bin")
      .toString();
  public static final String defaultDateNerURLStr = PontusNLPProcessor.class.getResource("/en-ner-date.bin").toString();
  public static final String defaultMoneyNerURLStr = PontusNLPProcessor.class.getResource("/en-ner-money.bin")
      .toString();
  public static final String defaultOrganizationNerURLStr = PontusNLPProcessor.class
      .getResource("/en-ner-organization.bin").toString();
  public static final String defaultTimeNerURLStr = PontusNLPProcessor.class.getResource("/en-ner-time.bin").toString();

  public static final String TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL =
      "{\n" + "  \"person\":       \"" + defaultPersonNerURLStr + "\"\n" + " ,\"location\":     \""
          + defaultLocationNerURLStr + "\"\n" + " ,\"date\":         \"" + defaultDateNerURLStr + "\"\n"
          + " ,\"money\":        \"" + defaultMoneyNerURLStr + "\"\n" + " ,\"organization\": \""
          + defaultOrganizationNerURLStr + "\"\n" + " ,\"time\":         \"" + defaultTimeNerURLStr + "\"\n" + "}";

  public final PropertyDescriptor TOKEN_NAME_FINDER_MODEL_JSON_PROP = new PropertyDescriptor.Builder()
      .name(TOKEN_NAME_FINDER_MODEL_JSON).description(
          "A JSON Object with the data types to be found, and a URL pointing to the model files (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin)")
      .addValidator(tokenNameFinderModelModelJSONValidator).expressionLanguageSupported(true).required(true)
      .defaultValue(TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL).dynamic(true).build();

  public static final String DATA_TO_PARSE = "Data to Parse";

  public final PropertyDescriptor DATA_TO_PARSE_PROP = new PropertyDescriptor.Builder().name(DATA_TO_PARSE)
      .displayName("Text to be processed").expressionLanguageSupported(true).description(
          "Text to parse, such as a Tweet, or a large set of phrases or sentences, if empty or null, the data will be read from the flowfile.")
      .addValidator(new StandardValidators.StringLengthValidator(0, 20000000)).defaultValue("").required(false).build();

  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
      .description("Successfully extracted values.").build();

  public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
      .description("Failed to extract values.").build();

  protected List<PropertyDescriptor> descriptors;

  protected Set<Relationship> relationships;

  protected Gson gson = new Gson();

  protected ComponentLog logger;

  protected String getInputData(final FlowFile flowFile, final ProcessSession session, final ProcessContext context)
  {

    String input = flowFile.getAttribute(DATA_TO_PARSE);

    if (input == null || input.length() == 0)
    {
      input = context.getProperty(DATA_TO_PARSE).evaluateAttributeExpressions(flowFile).getValue();

    }

    // if they pass in a sentence do that instead of flowfile
    if (input == null || input.length() == 0)
    {
      final AtomicReference<String> contentsRef = new AtomicReference<>(null);

      session.read(flowFile, data -> {
        final String contents = IOUtils.toString(data, "UTF-8");
        contentsRef.set(contents);
      });

      // use this as our text
      if (contentsRef.get() != null)
      {
        input = contentsRef.get();
      }
    }

    return input;

  }

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(THRESHOLD_PROB_PROP);

    descriptors.add(TOKEN_NAME_FINDER_MODEL_JSON_PROP);
    descriptors.add(REGEX_MODEL_JSON_PROP);
    descriptors.add(TOKENIZER_MODEL_JSON_PROP);

    //    descriptors.add(DICTIONARY_MODEL_JSON_PROP);
//    descriptors.add(SENTENCE_MODEL_JSON_PROP);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = context.getLogger();

    try
    {
      tokenNameFinderModelModelJSONValidator.createModels(TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);
    }
    catch (IOException | InvocationTargetException | NoSuchMethodException | URISyntaxException | InstantiationException | IllegalAccessException e)
    {
      logger.error("Failed to create Token Name Finder Models; error: " + e.getMessage());
      e.printStackTrace();
    }

    try
    {
      tokenizerModelModelJSONValidator.createModels(TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    }
    catch (IOException | InvocationTargetException | NoSuchMethodException | URISyntaxException | InstantiationException | IllegalAccessException e)
    {
      logger.error("Failed to create Token Name Finder Models; error: " + e.getMessage());
      e.printStackTrace();
    }

    try
    {
      dictionaryJSONValidator.createModels(DICTIONARY_MODEL_JSON_DEFAULT_VAL);

    }
    catch (Exception e)
    {
      logger.error("Failed to create Dictionary Models; error: " + e.getMessage());
      e.printStackTrace();

    }

    try
    {
      regexJSONValidator.createModels(REGEX_MODEL_JSON_DEFAULT_VAL);

    }
    catch (Exception e)
    {
      logger.error("Failed to create Dictionary Models; error: " + e.getMessage());
      e.printStackTrace();

    }

    try
    {
      sentenceModelModelJSONValidator.createModels(SENTENCE_MODEL_JSON_DEFAULT_VAL);
    }
    catch (Exception e)
    {
      logger.error("Failed to create Sentence Models; error: " + e.getMessage());
      e.printStackTrace();

    }
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(THRESHOLD_PROB_PROP))
    {

      thresholdProb = Double.parseDouble(newValue);
    }

    if (descriptor.equals(TOKEN_NAME_FINDER_MODEL_JSON_PROP))
    {
      try
      {
        tokenNameFinderModelModelJSONValidator.createModels(newValue);
      }
      catch (IOException | InvocationTargetException | NoSuchMethodException | URISyntaxException | InstantiationException | IllegalAccessException e)
      {
        logger.error("Failed to create Token Name Finder Models; error: " + e.getMessage());

      }
    }


    if (descriptor.equals(SENTENCE_MODEL_JSON_PROP))
    {
      try
      {
        sentenceModelModelJSONValidator.createModels(newValue);
      }
      catch (IOException | InvocationTargetException | NoSuchMethodException | URISyntaxException | InstantiationException | IllegalAccessException e)
      {
        logger.error("Failed to create Sentence Models; error: " + e.getMessage());

      }
    }


    if (descriptor.equals(TOKENIZER_MODEL_JSON_PROP))
    {
      try
      {
        tokenizerModelModelJSONValidator.createModels(newValue);
      }
      catch (IOException | InvocationTargetException | NoSuchMethodException | URISyntaxException | InstantiationException | IllegalAccessException e)
      {
        logger.error("Failed to create Tokenenizer Models; error: " + e.getMessage());

      }
    }

    if (descriptor.equals(REGEX_MODEL_JSON_PROP))
    {
      try
      {
        regexJSONValidator.createModels(newValue);
      }
      catch (IOException e)
      {
        logger.error("Failed to create REGEX Models; error: " + e.getMessage());

      }
    }

  }

  @Override public Set<Relationship> getRelationships()
  {
    return this.relationships;
  }

  @Override public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return descriptors;
  }

  @OnScheduled public void onScheduled(final ProcessContext context)
  {
    return;
  }

  protected FlowFile addResultsToFlowFile (FlowFile flowFile, ProcessSession session, Map<String, Set<String>> retVals)
  {
    for (Map.Entry<String, Set<String>> pair : retVals.entrySet())
    {
      String attribName = "nlp_res_" + pair.getKey().toLowerCase();
      String currData = flowFile.getAttribute(attribName);

      String finalVal;
      if (currData != null )
      {
        Set<String> currSet = gson.fromJson(currData,Set.class);
        currSet.addAll(pair.getValue());
        finalVal = gson.toJson(currSet);
      }
      else
      {
        finalVal = gson.toJson(pair.getValue());
      }

      flowFile = session
          .putAttribute(flowFile, attribName,finalVal );
    }

    return flowFile;
  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();
    if (flowFile == null)
    {
      flowFile = session.create();
    }
    try
    {
      String input = getInputData(flowFile, session, context);

      Map<String, Set<String>> retVals = new HashMap<>();

      Map<String, TokenizerModel> tokenizerModels = tokenizerModelModelJSONValidator.getModelMap();

      for (Map.Entry<String, TokenizerModel> pairTokenizer : tokenizerModels.entrySet())
      {
        TokenizerModel tokenModel = pairTokenizer.getValue();

        TokenizerME tokenizer = new TokenizerME(tokenModel);
        // Split the sentence into tokens
        String[] tokens = tokenizer.tokenize(input);


        processTokenNameFinder(tokens, retVals);
        //          processDictionary(tokens,retVals);
        processRegex(input, retVals);

      }

      flowFile =  addResultsToFlowFile(flowFile,session,retVals);


      session.transfer(flowFile, REL_SUCCESS);
      session.commit();
    }
    catch (final Throwable t)
    {
      getLogger().error("Unable to process NLP Processor file " + t.getLocalizedMessage());
      getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
      session.transfer(flowFile, REL_FAILURE);
      session.commit();
    }
  }

  protected void processTokenNameFinder(String[] tokens, Map<String, Set<String>> retVals)
  {
    Map<String, TokenNameFinderModel> tokenNameFinderModelMap = tokenNameFinderModelModelJSONValidator.getModelMap();

    for (Map.Entry<String, TokenNameFinderModel> pair : tokenNameFinderModelMap.entrySet())
    {

      Set<String> retValSet = retVals.computeIfAbsent(pair.getKey(), k -> new HashSet<>());

      TokenNameFinderModel tnfModel = pair.getValue();

      // Create a NameFinder using the model
      NameFinderME finder = new NameFinderME(tnfModel);

      // Find the names in the tokens and return Span objects
      Span[] nameSpans = finder.find(tokens);

      double[] probs = finder.probs(nameSpans);

      String[] spanns = Span.spansToStrings(nameSpans, tokens);
      for (int i = 0; i < spanns.length; i++)
      {
        if (probs[i] > thresholdProb)
        {
          retValSet.add(spanns[i]);
        }
      }
      finder.clearAdaptiveData();

    }

  }

  protected void processDictionary(String[] tokens, Map<String, Set<String>> retVals)
  {
    Map<String, Dictionary> tokenNameFinderModelMap = dictionaryJSONValidator.getModelMap();

    for (Map.Entry<String, Dictionary> pair : tokenNameFinderModelMap.entrySet())
    {

      Set<String> retValSet = retVals.computeIfAbsent(pair.getKey(), k -> new HashSet<>());

      Dictionary dic = pair.getValue();

      // Create a NameFinder using the model
      DictionaryNameFinder finder = new DictionaryNameFinder(dic);

      //      finder.
      // Find the names in the tokens and return Span objects
      Span[] nameSpans = finder.find(tokens);

      //      double[] probs = finder.probs(nameSpans);

      String[] spanns = Span.spansToStrings(nameSpans, tokens);
      retValSet.addAll(Arrays.asList(spanns));

    }

  }

  protected void processRegex(String text, Map<String, Set<String>> retVals)
  {
    Map<String, Pattern[]> tokenNameFinderModelMap = regexJSONValidator.getModelMap();

    for (Map.Entry<String, Pattern[]> pair : tokenNameFinderModelMap.entrySet())
    {

      Set<String> retValSet = retVals.computeIfAbsent(pair.getKey(), k -> new HashSet<>());

      Pattern[] dic = pair.getValue();

      // Create a NameFinder using the model
      RegexNameFinder finder = new RegexNameFinder(dic, pair.getKey());

      // Find the names in the tokens and return Span objects
      Span[] nameSpans = finder.find(text);

      //      double[] probs = finder.probs(nameSpans);

      String[] spanns = Span.spansToStrings(nameSpans, text);
      retValSet.addAll(Arrays.asList(spanns));

    }

  }
}
