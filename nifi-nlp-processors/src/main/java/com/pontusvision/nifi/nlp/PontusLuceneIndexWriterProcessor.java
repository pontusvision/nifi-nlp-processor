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

import com.google.gson.stream.JsonReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Tags({
    "Pontus Vision", "NLP", "Lucene", "Classification", "natural language processing", "writer" })
@CapabilityDescription("Lucene Index Writer - used to create Lucene indices that can be later used as dictionaries for classification of structured data") @SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") })
@WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }

)
public class PontusLuceneIndexWriterProcessor
    extends PontusProcessorBase
{

  protected Validator dictionaryJSONValidator = new BaseJSONValidator();

  protected Directory index;

  IndexWriterConfig config;
  Analyzer          analyzer = new BrazilianAnalyzer();

  // user name

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(INDEX_URI_PROP);
    descriptors.add(LUCENE_TYPE_PROP);
    //    descriptors.add(USER_NAME_PROP);
    //    descriptors.add(PASSWORD_PROP);
    //    descriptors.add(CUSTOM_MODEL_PROP);
    //    descriptors.add(VERSION_PROP);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }

  public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                 final String newValue)
  {

  }

  public IndexWriter init(ProcessContext context, FlowFile flowFile) throws URISyntaxException, IOException
  {

    //    context.getProperty(DICTIONARY_MODEL_JSON_PROP).getValue();

    if (this.index == null)
    {
      URI uri = new URI(context.getProperty(INDEX_URI_PROP).evaluateAttributeExpressions(flowFile).getValue());

      Path path = Paths.get(uri);

      this.index = new MMapDirectory(path);

      this.config = new IndexWriterConfig(analyzer);

    }
    IndexWriter writer = new IndexWriter(index, config);

    return writer;

  }

  public static void addDocument(IndexWriter writer, String data, LuceneTextFieldType type ) throws IOException
  {
    Document       doc            = new Document();
    IndexableField indexableField = type == LuceneTextFieldType.LUCENE_TEXT ?
        new TextField("data", new StringReader(data)):
        new StringField("data", data, Field.Store.YES)
        ;

    doc.add(indexableField);
    writer.addDocument(doc);

  }

  @OnStopped
  public void onStopped(ProcessContext context)
  {
    this.index = null;
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
      LuceneTextFieldType textFieldType = LuceneTextFieldType.valueOf(
          context.getProperty(LUCENE_TYPE_PROP).evaluateAttributeExpressions(flowFile).getValue());

      IndexWriter writer = init(context, flowFile);

      if (input.startsWith("["))
      {
        JsonReader reader = gson.newJsonReader(new StringReader(input));
        reader.setLenient(true);
        while (reader.hasNext())
        {
          addDocument(writer, reader.nextString(), textFieldType);
        }
      }

      else if (input.contains("\n"))
      {
        String[] lines = input.split("\n");
        for (String line : lines)
        {
          addDocument(writer, line, textFieldType);
        }
      }
      writer.close();
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
