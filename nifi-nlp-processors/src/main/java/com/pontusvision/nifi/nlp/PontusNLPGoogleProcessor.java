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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.language.v1.*;
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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

@Tags({
    "Pontus Vision, nlpprocessor, Google, nlp, natural language processing" }) @CapabilityDescription("Run OpenNLP Natural Language Processing against Google for Name, Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") }) @WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }) public class PontusNLPGoogleProcessor
    extends PontusNLPProcessor
{

  // user name

  public static Boolean isWindows =System.getProperty("os.name").toLowerCase().contains("windows");
  public static String GOOGLE_JSON_CREDS_URL_PROP_DEFAULT = isWindows? "file:/c:/work/creds.json": "file:///root/work2/creds.json";


//  static
//  {
//    try
//    {
//      GOOGLE_JSON_CREDS_URL_PROP_DEFAULT = isWindows? new File("c:\\work\\creds.json").toURI().toURL().toString(): "file:///tmp/creds.json";
//    }
//    catch (MalformedURLException e)
//    {
//      e.printStackTrace();
//    }
//  }

  public String credsUrl = GOOGLE_JSON_CREDS_URL_PROP_DEFAULT;
  public static final PropertyDescriptor GOOGLE_JSON_CREDS_URL_PROP = new PropertyDescriptor.Builder()
      .name("Google credentials").description("The URL to the google credentials json file; see https://cloud.google.com/storage/docs/authentication?hl=en#service_accounts for instructions how to create the creds.")
      .addValidator( StandardValidators.createURLorFileValidator()).expressionLanguageSupported(true).required(true)
      .defaultValue(GOOGLE_JSON_CREDS_URL_PROP_DEFAULT).dynamic(true).build();

  LanguageServiceClient service;

  protected void initService() throws IOException
  {

    LanguageServiceSettings languageServiceSettings = LanguageServiceSettings.newBuilder()
        .setCredentialsProvider(() -> ServiceAccountCredentials.fromStream(new URL(credsUrl).openStream())).build();

    service = LanguageServiceClient.create(languageServiceSettings);

  }

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(THRESHOLD_PROB_PROP);
    descriptors.add(GOOGLE_JSON_CREDS_URL_PROP);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = context.getLogger();
    try
    {
      initService();
    }
    catch (IOException e)
    {
      logger.error("Failed to initialize the google API service; error: " + e.getMessage());
      e.printStackTrace();
    }

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(GOOGLE_JSON_CREDS_URL_PROP))
    {
      credsUrl = newValue;
    }

    try
    {
      initService();
    }
    catch (IOException e)
    {
      logger.error("Failed to initialize the google API service; error: " + e.getMessage());
      e.printStackTrace();
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

      Document doc = Document.newBuilder().setContent(input).setType(Document.Type.PLAIN_TEXT).build();

      // Detects the sentiment of the text
      List<Entity> entities = service.analyzeEntities(doc, EncodingType.UTF8).getEntitiesList();

      Map<String, Set<String>> finalResults = new HashMap<>();

      for (int i = 0, ilen = entities.size(); i < ilen; i++)
      {
        Entity res = entities.get(i);

        double relevance = res.getSalience();
        if (relevance > thresholdProb)
        {
          String entityType = res.getType().name();

          Set<String> retValSet = finalResults.computeIfAbsent(entityType, k -> new HashSet<>());
          retValSet.add(res.getName());

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
