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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedical;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedicalClient;
import com.amazonaws.services.comprehendmedical.model.DetectPHIRequest;
import com.amazonaws.services.comprehendmedical.model.DetectPHIResult;
import com.amazonaws.services.comprehendmedical.model.Entity;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.*;

@Tags({
    "Pontus Vision, nlpprocessor, AWS Comprehend Medical, nlp, natural language processing" }) @CapabilityDescription("Run OpenNLP Natural Language Processing against AWS Comprehend Medical , Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") }) @WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") })
public class PontusNLPAWSComprehendMedicalProcessor
    extends PontusNLPProcessor
{

  // user name

  public              String             awsAccessKey        = "";
  public static final PropertyDescriptor AWS_ACCESS_KEY_PROP = new PropertyDescriptor
      .Builder()
      .name("AWS Access Key File")
      .description("File that has the AWS Access Key")
      .addValidator(FILE_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .required(true)
      .defaultValue("/run/secrets/AWS_ACCESS_KEY")
      .dynamic(true)
      .sensitive(true).build();

  // password
  public String awsSecret = "";

  public static final PropertyDescriptor AWS_SECRET_PROP = new PropertyDescriptor
      .Builder()
      .name("AWS Secret")
      .description("The file that has the AWS Secret")
      .addValidator(FILE_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true)
      .defaultValue("/run/secrets/AWS_SECRET")
      .dynamic(true)
      .sensitive(true).build();

  // region
  public String awsRegion = "";

  public static final PropertyDescriptor AWS_REGION_PROP = new PropertyDescriptor
      .Builder()
      .name("AWS Region")
      .description("The AWS Region")
      .addValidator(
          Validator.VALID)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).required(true)
      .defaultValue("")
      .dynamic(true)
      .sensitive(true).build();

  AWSCredentialsProvider credentials;

  AWSComprehendMedical awsComprehendMedical;

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(AWS_ACCESS_KEY_PROP);
    descriptors.add(AWS_SECRET_PROP);
    descriptors.add(AWS_REGION_PROP);
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(THRESHOLD_PROB_PROP);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = context.getLogger();

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  @OnScheduled
  public void onScheduled(final ProcessContext ctx) throws IOException
  {
    if (!alreadyInit)
    {

      awsAccessKey = readDataFromFileProperty(ctx, AWS_ACCESS_KEY_PROP);

      awsSecret = readDataFromFileProperty(ctx, AWS_SECRET_PROP);

      awsRegion = readDataFromFileProperty(ctx, AWS_REGION_PROP);

      credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecret));

      awsComprehendMedical = AWSComprehendMedicalClient.builder().withCredentials(credentials).withRegion(awsRegion)
                                                       .build();

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

      DetectPHIRequest request = new DetectPHIRequest();
      request.setText(input);

      DetectPHIResult results = awsComprehendMedical.detectPHI(request);
      //      result.getEntities().forEach(System.out::println);

      Map<String, Set<String>> finalResults = new HashMap<>();

      List<Entity> entities = results.getEntities();
      for (int i = 0, ilen = entities.size(); i < ilen; i++)
      {
        Entity res = entities.get(i);

        double relevance = res.getScore();
        if (relevance > thresholdProb)
        {
          String entityType = res.getType();

          Set<String> retValSet = finalResults.computeIfAbsent(entityType, k -> new HashSet<>());
          retValSet.add(res.getText());

        }

      }

      flowFile = addResultsToFlowFile(flowFile, session, finalResults);

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
