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
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public abstract class PontusProcessorBase
    extends AbstractProcessor
{

  public enum LuceneTextFieldType
  {
    LUCENE_TEXT,
    LUCENE_STRING
  };




  public final static Validator FILE_VALIDATOR = (subject, input, context) -> {

    boolean isValid = Paths.get(input).toFile().canRead();
    String explanation = isValid ?
        "Able to read from file" :
        "Failed to read file " + input + " for " + subject;
    ValidationResult.Builder builder = new ValidationResult.Builder();
    return builder.input(input).subject(subject).valid(isValid).explanation(explanation).build();
  };

  public static String readDataFromFileProperty(ProcessContext context, PropertyDescriptor prop)
      throws IOException
  {
    return new String(
        Files.readAllBytes(Paths.get(context.getProperty(prop).evaluateAttributeExpressions().getValue())),
        Charset.defaultCharset());
  }

  public static final String THRESHOLD_PROB             = "Probability Threshold";
  public static final String THRESHOLD_PROB_DEFAULT_VAL = "-0.01";

  Double thresholdProb = Double.parseDouble(THRESHOLD_PROB_DEFAULT_VAL);

  public final PropertyDescriptor THRESHOLD_PROB_PROP = new PropertyDescriptor
      .Builder()
      .name(THRESHOLD_PROB)
      .description("The threshold (0.0 - 1.0) that a particular value was a match; any values "
          + "lower than this will be discarded.")
      .addValidator(StandardValidators.NUMBER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .required(true)
      .defaultValue(THRESHOLD_PROB_DEFAULT_VAL)
      .build();

  public static final String             RESULTS_ATTRIB_PREFIX             = "Results Property Prefix";
  public static final String             RESULTS_ATTRIB_PREFIX_DEFAULT_VAL = "pg_nlp_res_";
  public final        PropertyDescriptor RESULTS_ATTRIB_PREFIX_PROP        = new PropertyDescriptor
      .Builder()
      .name(RESULTS_ATTRIB_PREFIX)
      .description("A prefix to the property values that match natural language processing categories.")
      .addValidator(new StandardValidators.StringLengthValidator(0, 1000))
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue(RESULTS_ATTRIB_PREFIX_DEFAULT_VAL).build();

  public static final String DATA_TO_PARSE = "Data to Parse";

  public final PropertyDescriptor DATA_TO_PARSE_PROP = new PropertyDescriptor
      .Builder()
      .name(DATA_TO_PARSE)
      .displayName("Text to be processed")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description("Text to parse, such as a Tweet, or a large set of phrases or sentences, if empty or"
          + " null, the data will be read from the flowfile.")
      .addValidator(new StandardValidators.StringLengthValidator(0, 20000000))
      .defaultValue("")
      .required(false)
      .build();

  public static final String QUERY_PATTERN = "QUERY PATTERN";

  public final PropertyDescriptor QUERY_PATTERN_PROP = new PropertyDescriptor
      .Builder()
      .name(QUERY_PATTERN)
      .displayName("Query Pattern for of lucene index")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description(
          "Java-style String.format pattern that replaces %s with the data to be queried.  The default value is to use the string as is (%s); "
              + "useful alternatives are ~%s, which will do a lucene-style similarity match.")
      .addValidator((subject, input, context) ->
          input.lastIndexOf("%s") == input.indexOf("%s")
              && input.contains("%s")
              && String.format(input, "FOOO").contains("FOOO") ?
              new ValidationResult.Builder().subject(subject).input(input).valid(true).build() :
              new ValidationResult.Builder()
                  .subject(subject)
                  .explanation(String.format("the format %s must contain one and only one %%s element inside.", input))
                  .input(input).build()
      )
      .defaultValue("%s")
      .required(true)
      .build();


  public static final String LUCENE_TYPE = "Lucene Type";

  public final PropertyDescriptor LUCENE_TYPE_PROP = new PropertyDescriptor
      .Builder()
      .name(LUCENE_TYPE)
      .displayName("Lucene type to store strings LUCENE_STRING, or LUCENE_TEXT")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description(
          "The type of Lucene index to create.  LUCENE_STRING is only good for exact matches, whereas LUCENE_TEXT is better at fuzzy queries.")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR
      )
      .allowableValues(LuceneTextFieldType.values())
      .defaultValue(LuceneTextFieldType.LUCENE_TEXT.name())
      .required(true)
      .build();





  public static final String REGEX_PATTERN = "REGEX PATTERN";

  public final PropertyDescriptor REGEX_PATTERN_PROP = new PropertyDescriptor
      .Builder()
      .name(REGEX_PATTERN)
      .displayName("Regex to search")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description("Java-style regex to match discovery elements.")
      .addValidator(StandardValidators.createRegexValidator(0, 100, true))
      .defaultValue(".*")
      .required(true)
      .build();

  public static final String INDEX_URI = "INDEX URI";

  public final PropertyDescriptor INDEX_URI_PROP = new PropertyDescriptor
      .Builder()
      .name(INDEX_URI)
      .displayName("URI of lucene index")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description("URI of lucene index for a single type of data.")
      .addValidator(StandardValidators.createURLorFileValidator())
      .defaultValue("file:///tmp/Person.Identity.Last_Name")
      .required(true)
      .build();

  public static final String DOMAIN = "DOMAIN";

  public final PropertyDescriptor DOMAIN_PROP = new PropertyDescriptor
      .Builder()
      .name(DOMAIN)
      .displayName("Domain (POLE entity name)")
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .description("Domain (e.g. Person.Identity.Last_Name, Location.Address.City)")
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .defaultValue("")
      .required(true)
      .build();


  public static final Relationship REL_SUCCESS = new Relationship
      .Builder()
      .name("success")
      .description("Successfully extracted values.")
      .build();

  public static final Relationship REL_FAILURE = new Relationship
      .Builder()
      .name("failure")
      .description("Failed to extract values.")
      .build();

  public static final String DICTIONARY_MODEL_JSON = "Dictionary Model in JSON";

  public static final String defaultPersonDictURLStr = PontusNLPProcessor.class.getResource("/en-dict-names.txt")
                                                                               .toString();

  public static final String    DICTIONARY_MODEL_JSON_DEFAULT_VAL = "{\"person\": \"" + defaultPersonDictURLStr + "\"}";
  protected           Validator dictionaryJSONValidator           = new DictionaryJSONValidator();

  public final PropertyDescriptor DICTIONARY_MODEL_JSON_PROP = new PropertyDescriptor
      .Builder()
      .name(DICTIONARY_MODEL_JSON)
      .description("A JSON Object with the data type to be processed, and a URL pointing to the "
          + "dictionary (e.g. http://opennlp.sourceforge.net/models-1.5/en-token.bin")
      .addValidator(dictionaryJSONValidator)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .required(true)
      .defaultValue(DICTIONARY_MODEL_JSON_DEFAULT_VAL).build();

  protected String                   resultsAttribPrefix = RESULTS_ATTRIB_PREFIX_DEFAULT_VAL;
  protected List<PropertyDescriptor> descriptors;

  protected Set<Relationship> relationships;

  protected Gson gson = new Gson();

  protected          ComponentLog logger;
  protected volatile boolean      alreadyInit = false;

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

    descriptors.add(RESULTS_ATTRIB_PREFIX_PROP);

    //    descriptors.add(DICTIONARY_MODEL_JSON_PROP);
    //    descriptors.add(SENTENCE_MODEL_JSON_PROP);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

  }

  @Override public Set<Relationship> getRelationships()
  {
    return this.relationships;
  }

  @Override public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return descriptors;
  }

  @OnStopped
  public void onStopped()
  {
    alreadyInit = false;

  }

  protected FlowFile addResultsToFlowFile(FlowFile flowFile, ProcessSession session, Map<String, Set<String>> retVals)
  {
    for (Map.Entry<String, Set<String>> pair : retVals.entrySet())
    {
      String attribName = resultsAttribPrefix + pair.getKey().toLowerCase();
      String currData   = flowFile.getAttribute(attribName);

      String finalVal;
      if (currData != null)
      {
        Set<String> currSet = gson.fromJson(currData, Set.class);
        currSet.addAll(pair.getValue());
        finalVal = gson.toJson(currSet);
      }
      else
      {
        finalVal = gson.toJson(pair.getValue());
      }

      flowFile = session
          .putAttribute(flowFile, attribName, finalVal);
    }

    return flowFile;
  }

}
