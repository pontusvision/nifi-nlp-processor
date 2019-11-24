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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.lucene.queryparser.classic.ParseException;
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

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

@Tags({
    "Pontus Vision", "NLP", "Regex", "Classification", "natural language processing", "reader", "Database" })
@CapabilityDescription("Regex tester - used to classify structured data against previously build data dictionaries")
@SeeAlso({}) @ReadsAttributes({
    @ReadsAttribute(attribute = "pg_rdb_col_metadata", description = "JSON text that has an input like this: {\n"
        + "\t\"colMetaData\": [\n"
        + "\t\t{\n"
        + "\t\t\t\"colName\": \"dept_no\",\n"
        + "\t\t\t\"primaryKeyName\": \"PRIMARY\",\n"
        + "\t\t\t\"foreignKeyName\": \"\",\n"
        + "\t\t\t\"typeName\": \"\",\n"
        + "\t\t\t\"colRemarks\": \"\",\n"
        + "\t\t\t\"isAutoIncr\": \"NO\",\n"
        + "\t\t\t\"isGenerated\": \"NO\",\n"
        + "\t\t\t\"octetLen\": 16,\n"
        + "\t\t\t\"ordinalPos\": 1,\n"
        + "\t\t\t\"defVal\": \"\",\n"
        + "\t\t\t\"colSize\": 4,\n"
        + "\t\t\t\"vals\": [\n"
        + "\t\t\t\t\"d009\",\n"
        + "\t\t\t\t\"d005\",\n"
        + "\t\t\t\t\"d002\",\n"
        + "\t\t\t\t\"d003\",\n"
        + "\t\t\t\t\"d001\",\n"
        + "\t\t\t\t\"d004\",\n"
        + "\t\t\t\t\"d006\",\n"
        + "\t\t\t\t\"d008\",\n"
        + "\t\t\t\t\"d007\"\n"
        + "\t\t\t]\n"
        + "\t\t}, ... ") })
@WritesAttributes({
    @WritesAttribute(attribute = "pg_discovery_col_data", description =
        "JSON output with discovery information like this:"
            + "{\n"
            + "  metadata: {\n"
            + "    columns: [\n"
            + "      {\n"
            + "        name: \"\", domain: \"\", frequency: 0.0, semanticDomains:\n"
            + "        [\n"
            + "          { id:\"\", frequency: 0.0 }\n"
            + "        ]\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}") }

)
public class PontusDiscoveryRegexDBClassifierProcessor
    extends PontusDiscoveryDBClassifierProcessor
{

  //1st way
  Pattern pattern = null; // Pattern.compile(".s");//. represents single character


  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(DOMAIN_PROP);
    descriptors.add(REGEX_PATTERN_PROP);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    //    service = new NaturalLanguageUnderstanding(watsonVersion);

  }


  @Override
  boolean isMatch (String data) throws IOException, ParseException
  {
    return pattern.matcher(data).matches();
  }


  @Override
  public Map<String, JsonObject> getSemanticsMap(ColMetadata[] colMetadataArray, String domain, String queryFormat)
      throws IOException, ParseException
  {

    Map<String, JsonObject> semanticsMap = new HashMap<>();

    for (ColMetadata colMetadata : colMetadataArray)
    {
      long foundNum = 0;
      long totalNum = 0;

      for (String candidate : colMetadata.vals)
      {
        totalNum++;
        foundNum += (isMatch(candidate) ? 1 : 0);
      }
      Double     percentage = (double) foundNum / (double) totalNum * 100.0;
      JsonObject semantic   = new JsonObject();

      semantic.addProperty("id", domain);
      semantic.addProperty("frequency", percentage);
      semanticsMap.put(colMetadata.getColName().trim(), semantic);

    }
    return semanticsMap;

  }

  //  Here is the discovery metadata output format:
  //        {
  //          metadata: {
  //            columns: [
  //              {
  //                name: "", domain: "", frequency: 0.0, semanticDomains:
  //                [
  //                  { id:"", frequency: 0.0 }
  //                ]
  //              }
  //            ]
  //          }
  //        }





  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();
    if (flowFile == null)
    {
      return;
    }
    try
    {
      String domain = context.getProperty(DOMAIN_PROP).evaluateAttributeExpressions(flowFile).getValue();

      String input = getInputData(flowFile, session, context);

      ColMetadataJSON colMetadataObj = gson.fromJson(input, ColMetadataJSON.class);

      String regexPattern = context.getProperty(REGEX_PATTERN_PROP).evaluateAttributeExpressions(flowFile).getValue();

      this.pattern = Pattern.compile(regexPattern);

      Map<String, JsonObject> semanticsMap = getSemanticsMap(colMetadataObj.colMetaData, domain, regexPattern);

      flowFile = upsertDiscoveryData(flowFile, session, semanticsMap, domain);

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
