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

import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.cloud.sdk.core.http.ServiceCall;
import com.ibm.cloud.sdk.core.security.basicauth.BasicAuthConfig;
import com.ibm.watson.natural_language_understanding.v1.NaturalLanguageUnderstanding;
import com.ibm.watson.natural_language_understanding.v1.model.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;

@Tags({
    "Pontus Vision", "NLP", "NER", "Named Entity Recognition", "IBM Watson", "natural language processing" })
@CapabilityDescription("Run OpenNLP Natural Language Processing against IBM Watson for Name, Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") })
@WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }

)
public class PontusNLPWatsonProcessor
    extends PontusNLPProcessor
{

  // user name

  public              String             userName       = null;
  public static final PropertyDescriptor USER_NAME_PROP = new PropertyDescriptor.Builder().name("Watson User Name File")
                                                                                          .description(
                                                                                              "The docker/k8s secrets file with the user name to use the Watson Service")
                                                                                          .addValidator(FILE_VALIDATOR)
                                                                                          .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                          .required(true).defaultValue(
          "/run/secrets/WATSON_USER_NAME").build();

  // password
  public String password = null;

  public static final PropertyDescriptor PASSWORD_PROP = new PropertyDescriptor.Builder().name("Watson Password File")
                                                                                         .description(
                                                                                             "The The docker/k8s secrets file with the password to use the Watson Service")
                                                                                         .addValidator(FILE_VALIDATOR)
                                                                                         .expressionLanguageSupported(
                                                                                             ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                         .required(true).defaultValue(
          "/run/secrets/WATSON_PASSWORD").dynamic(true).sensitive(true).build();

  public String watsonVersion = "2018-03-19";

  public final PropertyDescriptor VERSION_PROP = new PropertyDescriptor.Builder().name("Watson Version")
                                                                                 .description(
                                                                                     "The version of the Watson Service")
                                                                                 .addValidator(
                                                                                     new StandardValidators.StringLengthValidator(
                                                                                         0, 100))
                                                                                 .expressionLanguageSupported(
                                                                                     ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                 .required(true)
                                                                                 .defaultValue(watsonVersion)
                                                                                 .dynamic(true).build();

  public       String             customModel       = "";
  public final PropertyDescriptor CUSTOM_MODEL_PROP = new PropertyDescriptor.Builder().name("Custom Model")
                                                                                      .description(
                                                                                          "Optional Custom Model for Watson; if left empty, the default will be used.")
                                                                                      .addValidator(
                                                                                          new StandardValidators.StringLengthValidator(
                                                                                              0, 10000000))
                                                                                      .expressionLanguageSupported(
                                                                                          ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(false)
                                                                                      .defaultValue(customModel)
                                                                                      .dynamic(true).build();

  protected EntitiesOptions entitiesOptions;

  protected Features features;

  protected AnalyzeOptions.Builder analyzeOptsBuilder = new AnalyzeOptions.Builder();

  protected NaturalLanguageUnderstanding service;

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(THRESHOLD_PROB_PROP);
    descriptors.add(USER_NAME_PROP);
    descriptors.add(PASSWORD_PROP);
    descriptors.add(CUSTOM_MODEL_PROP);
    descriptors.add(VERSION_PROP);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = getLogger();

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                 final String newValue)
  {

  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) throws IOException
  {
    if (!alreadyInit)
    {
      watsonVersion = context.getProperty(VERSION_PROP).evaluateAttributeExpressions().getValue();
      userName = readDataFromFileProperty(context, USER_NAME_PROP);

      password = readDataFromFileProperty(context, PASSWORD_PROP);

      customModel = context.getProperty(CUSTOM_MODEL_PROP).evaluateAttributeExpressions().getValue();

      BasicAuthConfig conf = new BasicAuthConfig.Builder().username(userName).password(password).build();
      service = new NaturalLanguageUnderstanding(watsonVersion, conf);

      EntitiesOptions.Builder entitiesOptionsBuilder = new EntitiesOptions.Builder();

      entitiesOptionsBuilder.emotion(false).limit(250).sentiment(false).mentions(false);
      if (customModel != null && customModel.length() > 0)
      {
        entitiesOptionsBuilder.model(customModel);
      }

      entitiesOptions = entitiesOptionsBuilder.build();
      features = new Features.Builder().entities(entitiesOptions).build();

      alreadyInit = true;

    }
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

      analyzeOptsBuilder.text(input);

      analyzeOptsBuilder.features(features);

      ServiceCall<AnalysisResults> call    = service.analyze(analyzeOptsBuilder.build());
      Response<AnalysisResults>    results = call.execute();

      Map<String, Set<String>> finalResults = new HashMap<>();

      List<EntitiesResult> entities = results.getResult().getEntities();
      for (int i = 0, ilen = entities.size(); i < ilen; i++)
      {
        EntitiesResult res = entities.get(i);

        double relevance = res.getRelevance();
        if (relevance > thresholdProb)
        {
          String entityType = res.getType();

          Set<String> retValSet = finalResults.computeIfAbsent(entityType, k -> new HashSet<>());
          retValSet.add(res.getText());

        }

      }

      flowFile = addResultsToFlowFile(flowFile, session, finalResults);

      session.transfer(flowFile, REL_SUCCESS);
    }
    catch (final Throwable t)
    {
      getLogger().error("Unable to process NLP Processor file " + t.getLocalizedMessage());
      getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
      session.transfer(flowFile, REL_FAILURE);
    }
  }

}
