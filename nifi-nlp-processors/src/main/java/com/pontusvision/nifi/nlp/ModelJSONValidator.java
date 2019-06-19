package com.pontusvision.nifi.nlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import opennlp.tools.util.model.BaseModel;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ModelJSONValidator<T extends BaseModel> implements Validator
{

  Class<T> classObj;

  Map<String, T> modelMap = new ConcurrentHashMap<>();

  Set<String> dataModels = new HashSet<>();

  public ModelJSONValidator(Class<T> classObj)
  {
    this.classObj = classObj;
  }

  public Map<String, T> getModelMap()
  {
    return modelMap;
  }

  public Set<String> getDataModels()
  {
    return dataModels;
  }

  public T getModel(String modelType)
  {

    return modelMap.get(modelType);
  }

  protected T getModel(URL url)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException,
      URISyntaxException
  {
    //      File fil = new File(url.to);

    Constructor<T> ctor = classObj.getConstructor(URL.class);

    T model = ctor.newInstance(url);

    return model;

  }

  void destroyModels()
  {
    dataModels.clear();

    Iterator<Map.Entry<String, T>> it = modelMap.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, T> pair = it.next();
      modelMap.remove(pair.getKey());

      it.remove();
    }
    modelMap.clear();

  }

  @Override public ValidationResult validate(String subject, String input, ValidationContext context)
  {
    if (dataModels.isEmpty())
    {
      return validateJSONURLs(subject, input, context);
    }

    else
    {
      return VALID.validate(subject,input,context);
    }

  }

  public static ValidationResult validateJSONURLs(String subject, String input, ValidationContext context)
  {
    boolean valid = true;
    String error = "";
    String uriStr = "";
    String modelType = "";


    try
    {

      HashMap<String, String> result = new ObjectMapper().readValue(input, HashMap.class);

      Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<String, String> pair = it.next();

        modelType = pair.getKey();
        uriStr = pair.getValue();

        URL url = new URL(uriStr);
        InputStream stream = url.openStream();
        stream.close();

        it.remove(); // avoids a ConcurrentModificationException
      }

    }
    catch (Exception e)
    {
      valid = false;
      error = " Failed to load model for " + modelType + "With URI " + uriStr + ".  Error: " + e.getMessage();
    }

    //      StringBuilder err = new StringBuilder("Property ").append(subject).append("; ").append(error);
    return (new ValidationResult.Builder()).subject(subject).input(input).valid(valid).explanation(error).build();
  }

  public void createModels(String input)
      throws IOException, InvocationTargetException, NoSuchMethodException, URISyntaxException, InstantiationException,
      IllegalAccessException
  {
    String uriStr;
    String modelType;


    HashMap<String, String> result = new ObjectMapper().readValue(input, HashMap.class);

    Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, String> pair = it.next();

      modelType = pair.getKey();
      uriStr = pair.getValue();

      T model = getModel(new URL(uriStr));

      if (model != null)
      {
        modelMap.put(modelType, model);
        dataModels.add(modelType);
      }
      System.out.println("Getting model for modelType " + modelType);

      it.remove(); // avoids a ConcurrentModificationException
    }

  }
}
