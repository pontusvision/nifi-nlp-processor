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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
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
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Tags({
    "Pontus Vision", "NLP", "Lucene", "Classification", "natural language processing", "reader" })
@CapabilityDescription("Lucene Index Reader - used to classify structured data against previously build data dictionaries")
@SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "text", description = "text coming in") })
@WritesAttributes({
    @WritesAttribute(attribute = "nlp_res_name, nlp_res_location, nlp_res_date", description = "nlp names, locations, dates") }

)
public class PontusLuceneIndexReaderProcessor
    extends PontusProcessorBase
{


  Analyzer analyzer = new BrazilianAnalyzer();
  protected Directory index;
  IndexSearcher        searcher;

  // user name





  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(INDEX_URI_PROP);
    descriptors.add(DATA_TO_PARSE_PROP);
    descriptors.add(QUERY_PATTERN_PROP);

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



  public IndexSearcher init(ProcessContext context, FlowFile flowFile) throws URISyntaxException, IOException
  {

    if (this.searcher == null)
    {
      URI uri = new URI(context.getProperty(INDEX_URI_PROP).evaluateAttributeExpressions(flowFile).getValue());

      Path path = Paths.get(uri);

      this.index = new MMapDirectory(path);

      Directory index = new MMapDirectory(path);
      IndexReader          reader      = DirectoryReader.open(index);

      this.searcher    = new IndexSearcher(reader);

    }

    return this.searcher;

  }

  boolean isMatch (String data) throws IOException, ParseException
  {
    Query q = new QueryParser("data", analyzer).parse(data);
    TopDocs res = searcher.search(q, 1);
    long numHits = res.scoreDocs.length;
    return numHits > 0;
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
      this.searcher = init(context,flowFile);

      String input = getInputData(flowFile, session, context);
      long foundNum = 0;
      long totalNum = 0;

      if (input.startsWith("["))
      {
        List<String> candidates =  gson.fromJson(input, List.class);

        for (String candidate: candidates)
        {
          totalNum ++;
          foundNum += (isMatch(candidate))? 1 : 0;
        }
      }
      else if (input.contains("\n"))
      {
        String[] lines = input.split("\n");
        for (String line : lines)
        {
          totalNum ++;
          foundNum += (isMatch(line))? 1 : 0;
        }
      }

      Double percentage = (double)foundNum/(double)totalNum * 100.0;

      System.out.println ("Percent found = " + percentage);

      flowFile = session.putAttribute(flowFile, "PERCENTAGE_MATCH", percentage.toString());

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
