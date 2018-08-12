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

import com.ibm.watson.developer_cloud.http.ServiceCall;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.NaturalLanguageUnderstanding;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({
    "Pontus Vision, nlpprocessor, IBM Watson, nlp, natural language processing" }) @CapabilityDescription("Run OpenNLP Natural Language Processing against IBM Watson for Name, Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") }) @WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }) public class PontusNLPWatsonProcessor
    extends PontusNLPProcessor
{

  // user name

  public String userName = "";
  public static final PropertyDescriptor USER_NAME_PROP = new PropertyDescriptor.Builder().name("Watson User Name")
      .description("The user name to use the Watson Service")
      .addValidator(new StandardValidators.StringLengthValidator(0, 100)).expressionLanguageSupported(true)
      .required(true).defaultValue("").dynamic(true).build();

  // password
  public String password = "";

  public static final PropertyDescriptor PASSWORD_PROP = new PropertyDescriptor.Builder().name("Watson Password")
      .description("The password to use the Watson Service")
      .addValidator(new StandardValidators.StringLengthValidator(0, 100)).expressionLanguageSupported(true)
      .required(true).defaultValue("").dynamic(true).sensitive(true).build();

  public String watsonVersion = "2018-03-19";

  public final PropertyDescriptor VERSION_PROP = new PropertyDescriptor.Builder().name("Watson Version")
      .description("The version of the Watson Service")
      .addValidator(new StandardValidators.StringLengthValidator(0, 100)).expressionLanguageSupported(true)
      .required(true).defaultValue(watsonVersion).dynamic(true).build();

  public String customModel = "";
  public final PropertyDescriptor CUSTOM_MODEL_PROP = new PropertyDescriptor.Builder().name("Custom Model")
      .description("Optional Custom Model for Watson; if left empty, the default will be used.")
      .addValidator(new StandardValidators.StringLengthValidator(0, 10000000)).expressionLanguageSupported(true)
      .required(false).defaultValue(customModel).dynamic(true).build();

  protected EntitiesOptions entitiesOptions;

  protected Features features;

  protected AnalyzeOptions.Builder analyzeOptsBuilder = new AnalyzeOptions.Builder();

  protected NaturalLanguageUnderstanding service;

  protected void initFeatures()
  {
    EntitiesOptions.Builder entitiesOptionsBuilder = new EntitiesOptions.Builder();

    entitiesOptionsBuilder.emotion(false).limit(250).sentiment(false).mentions(false);
    if (customModel != null && customModel.length() > 0)
    {
      entitiesOptionsBuilder.model(customModel);
    }

    entitiesOptions = entitiesOptionsBuilder.build();
    features = new Features.Builder().entities(entitiesOptions).build();

  }

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

    logger = context.getLogger();

    initFeatures();
    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(VERSION_PROP))
    {
      watsonVersion = newValue;
    }

    else if (descriptor.equals(USER_NAME_PROP))
    {
      userName = newValue;
    }
    else if (descriptor.equals(PASSWORD_PROP))
    {
      password = newValue;
    }
    else if (descriptor.equals(CUSTOM_MODEL_PROP))
    {
      customModel = newValue;
      initFeatures();
    }

    service = new NaturalLanguageUnderstanding(watsonVersion, userName, password);

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

      ServiceCall<AnalysisResults> call = service.analyze(analyzeOptsBuilder.build());
      AnalysisResults results = call.execute();

      Map<String, Set<String>> finalResults = new HashMap<>();

      List<EntitiesResult> entities = results.getEntities();
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

      flowFile =  addResultsToFlowFile(flowFile,session,finalResults);

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

}
