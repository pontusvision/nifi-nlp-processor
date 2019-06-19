package com.pontusvision.nifi.nlp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexJSONValidator implements Validator
{
  Map<String, Pattern[]> modelMap = new ConcurrentHashMap<>();

  Set<String> dataModels = new HashSet<>();

  public Map<String, Pattern[]> getModelMap()
  {
    return modelMap;
  }

  public Set<String> getDataModels()
  {
    return dataModels;
  }

  public Pattern[] getModel(String modelType)
  {

    return modelMap.get(modelType);
  }

  void destroyModels()
  {
    dataModels.clear();

    Iterator<Map.Entry<String, Pattern[]>> it = modelMap.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, Pattern[]> pair = it.next();
      modelMap.remove(pair.getKey());
      it.remove();
    }
    modelMap.clear();

  }

  public void createModels(String input) throws IOException
  {
    String regexStr = "";
    String modelType = "";

    try
    {
      HashMap<String, String> result = new ObjectMapper().readValue(input, HashMap.class);

      Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<String, String> pair = it.next();

        modelType = pair.getKey();
        regexStr = pair.getValue();

        Pattern model = Pattern.compile(regexStr,Pattern.MULTILINE|Pattern.CASE_INSENSITIVE);

        modelMap.put(modelType, new Pattern[] { model });
        dataModels.add(modelType);

        it.remove(); // avoids a ConcurrentModificationException
      }

    }catch (Exception e){
      throw new IOException( "Failed to load model for " + modelType + "With pattern " + regexStr + ".  Error: " + e.getMessage(), e);
    }
  }

  @Override public ValidationResult validate(String subject, String input, ValidationContext context)
  {
    boolean valid = true;
    String error = "";

    try
    {
      if (dataModels.isEmpty())
      {
        createModels(input);
      }
    }
    catch (IOException |PatternSyntaxException e)
    {
      valid = false;
      error = e.getMessage();
      destroyModels();
    }

    //      StringBuilder err = new StringBuilder("Property ").append(subject).append("; ").append(error);
    return (new ValidationResult.Builder()).subject(subject).input(input).valid(valid).explanation(error).build();
  }
}
