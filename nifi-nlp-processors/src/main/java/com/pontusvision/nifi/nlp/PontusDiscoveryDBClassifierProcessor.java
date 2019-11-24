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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Tags({
    "Pontus Vision", "NLP", "Lucene", "Classification", "natural language processing", "reader", "Database" })
@CapabilityDescription("Lucene Index Reader - used to classify structured data against previously build data dictionaries")
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
public class PontusDiscoveryDBClassifierProcessor
    extends PontusLuceneIndexReaderProcessor
{
/*
   Takes in an input like this:
{
	"colMetaData": [
		{
			"colName": "dept_no",
			"primaryKeyName": "PRIMARY",
			"foreignKeyName": "",
			"typeName": "",
			"colRemarks": "",
			"isAutoIncr": "NO",
			"isGenerated": "NO",
			"octetLen": 16,
			"ordinalPos": 1,
			"defVal": "",
			"colSize": 4,
			"vals": [
				"d009",
				"d005",
				"d002",
				"d003",
				"d001",
				"d004",
				"d006",
				"d008",
				"d007"
			]
		},
		{
			"colName": "dept_name",
			"primaryKeyName": "",
			"foreignKeyName": "",
			"typeName": "",
			"colRemarks": "",
			"isAutoIncr": "NO",
			"isGenerated": "NO",
			"octetLen": 160,
			"ordinalPos": 2,
			"defVal": "",
			"colSize": 40,
			"vals": [
				"Customer Service",
				"Development",
				"Finance",
				"Human Resources",
				"Marketing",
				"Production",
				"Quality Management",
				"Research",
				"Sales"
			]
		}
	],
	"tableCatalog": "employees",
	"tableName": "departments",
	"fqn": "employees.departments",
	"tableType": "TABLE",
	"tableRemarks": ""
}


*/

  // user name

  protected String getInputData(final FlowFile flowFile, final ProcessSession session, final ProcessContext context)
  {
    return flowFile.getAttribute("pg_rdb_col_metadata");
  }

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
        foundNum += (isMatch(String.format(queryFormat, candidate))) ? 1 : 0;
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
  public FlowFile upsertDiscoveryData(FlowFile flowFile, ProcessSession session, Map<String, JsonObject> semanticsMap,
                                      String domain)
  {
    final JsonArray  columns;
    final JsonObject metadata;
    final String     currDiscovery = flowFile.getAttribute("pg_discovery_col_data");
    final JsonObject discovery;

    if (currDiscovery == null)
    {
      columns = new JsonArray();
      metadata = new JsonObject();
      metadata.add("columns", columns);

      //                name: "", domain: "", frequency: 0.0, semanticDomains:

      semanticsMap.forEach((colName, semantic) -> {
        JsonObject column = new JsonObject();

        column.addProperty("name", colName);
        Double    frequency       = semantic.get("frequency").getAsDouble();
        if (frequency > 0.0)
        {
          JsonArray semanticDomains = new JsonArray();
          column.add("semanticDomains", semanticDomains);

          semanticDomains.add(semantic);
          column.addProperty("domain", domain);
          column.addProperty("frequency", frequency);
        }

        columns.add(column);

      });

      discovery = new JsonObject();
      discovery.add("metadata", metadata);
    }
    else
    {
      discovery = gson.fromJson(currDiscovery, JsonObject.class);
      metadata = discovery.getAsJsonObject("metadata");
      columns = metadata.getAsJsonArray("columns");
      columns.forEach(column -> {
        JsonElement nameElement = ((JsonObject) column).get("name");
        if (nameElement != null)
        {
          //   name: "", domain: "", frequency: 0.0, semanticDomains:

          String     name     = nameElement.getAsString();
          JsonObject semantic = semanticsMap.get(name);
          if (semantic != null)
          {
            JsonElement currFrequencyElement = ((JsonObject) column).get("frequency");
            Double      currFrequency        = 0.0;
            if (currFrequencyElement != null)
            {
              currFrequency = currFrequencyElement.getAsDouble();
            }

            Double frequency = semantic.get("frequency").getAsDouble();

            if (currFrequency < frequency)
            {
              ((JsonObject) column).addProperty("domain", domain);
              ((JsonObject) column).addProperty("frequency", frequency);
              JsonArray semanticDomains = ((JsonObject) column).getAsJsonArray("semanticDomains");
              if (semanticDomains == null){
                semanticDomains = new JsonArray();
                ((JsonObject) column).add("semanticDomains",semanticDomains);
              }
              semanticDomains.add(semantic);

            }

          }

        }

      });

    }

    flowFile = session.putAttribute(flowFile, "pg_discovery_col_data", gson.toJson(discovery));

    return flowFile;
  }

  class ColMetadataJSON
  {
    public ColMetadata[] colMetaData;
  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();
    if (flowFile == null)
    {
      return;
    }
    try
    {
      String indexURIStr = context.getProperty(INDEX_URI_PROP).evaluateAttributeExpressions(flowFile).getValue();
      String domain      = indexURIStr.substring(indexURIStr.lastIndexOf("/") + 1);

      this.searcher = init(context, flowFile);

      String input = getInputData(flowFile, session, context);

      ColMetadataJSON colMetadataObj = gson.fromJson(input, ColMetadataJSON.class);

      String queryFormatter = context.getProperty(QUERY_PATTERN_PROP).evaluateAttributeExpressions(flowFile).getValue();

      Map<String, JsonObject> semanticsMap = getSemanticsMap(colMetadataObj.colMetaData, domain, queryFormatter);

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
