package com.pontusvision.nifi.nlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import opennlp.tools.dictionary.Dictionary;
import opennlp.tools.util.StringList;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DictionaryJSONValidator implements Validator
{
  Map<String, Dictionary> modelMap = new ConcurrentHashMap<>();

  Set<String> dataModels = new HashSet<>();

  public Map<String, Dictionary> getModelMap()
  {
    return modelMap;
  }

  public Set<String> getDataModels()
  {
    return dataModels;
  }

  public Dictionary getModel(String modelType)
  {

    return modelMap.get(modelType);
  }

  protected Dictionary getModel(URL url) throws IOException
  {

    BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));

    Dictionary model = new Dictionary(false);

    String line;
    while ((line = reader.readLine()) != null)
    {

      model.put(new StringList(line));

    }

    return model;

  }

  void destroyModels()
  {
    Iterator<Map.Entry<String, Dictionary>> it = modelMap.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, Dictionary> pair = it.next();
      modelMap.remove(pair.getKey());
      it.remove();
    }
    modelMap.clear();

  }

  public void createModels(String input) throws IOException
  {
    String uriStr;
    String modelType;

    destroyModels();
    dataModels.clear();

    HashMap<String, String> result = new ObjectMapper().readValue(input, HashMap.class);

    Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, String> pair = it.next();

      modelType = pair.getKey();
      uriStr = pair.getValue();

      Dictionary model = getModel(new URL(uriStr));

      if (model != null)
      {
        modelMap.put(modelType, model);
        dataModels.add(modelType);
      }

      it.remove(); // avoids a ConcurrentModificationException
    }

  }

  @Override public ValidationResult validate(String subject, String input, ValidationContext context)
  {
    return ModelJSONValidator.validateJSONURLs(subject,input,context);
  }
}
