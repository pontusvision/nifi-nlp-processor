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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.util.*;

@Tags({
    "Pontus Vision", "NLP", "NER", "Named Entity Recognition", "SpaCy", "natural language processing" })
@CapabilityDescription("Run Natural Language Processing against SpaCy using REST for Name, Location, Date, Sentence, URL or any combination") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") }) @WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }) public class PontusNLPSpaCyProcessor
    extends PontusNLPProcessor
{

  // user name

  public       String             spacyURLStr    = null;
  public final PropertyDescriptor SPACY_REST_URL = new PropertyDescriptor
      .Builder()
      .name("SpaCy REST API URL")
      .description("URL to connect to the SpaCy REST service.")
      .addValidator(StandardValidators.URL_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .required(true).defaultValue("http://spacyapi:8085/ent").build();

  public       String             spacyModel  = "en";
  public final PropertyDescriptor SPACY_MODEL = new PropertyDescriptor
      .Builder()
      .name("SpaCy Language Model")
      .description("Language Model")
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .required(true).defaultValue("en").build();

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(SPACY_REST_URL);
    descriptors.add(SPACY_MODEL);
    descriptors.add(DATA_TO_PARSE_PROP);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = getLogger();

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  @OnScheduled
  public void onScheduled(final ProcessContext context)
  {
    if (!alreadyInit)
    {
      spacyModel = context.getProperty(SPACY_MODEL).evaluateAttributeExpressions().getValue();
      spacyURLStr = context.getProperty(SPACY_REST_URL).evaluateAttributeExpressions().getValue();

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

      HttpRequest request = new HttpRequest().url(spacyURLStr).method("POST")
                                             .responseFormat(HttpRequest.ResponseFormat.STRING);

      request.content(input.getBytes());

      HttpResponse resp = Http.send(request);

      JSONArray respJsonArray = new JSONArray(resp.getString());

      Map<String, Set<String>> finalResults = new HashMap<>();

      for (int i = 0, ilen = respJsonArray.length(); i < ilen; i++)
      {
        JSONObject obj        = respJsonArray.getJSONObject(i);
        String     entityType = obj.getString("type");

        String text = obj.getString("text");

        Set<String> retValSet = finalResults.computeIfAbsent(entityType, k -> new HashSet<>());
        retValSet.add(text);

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
