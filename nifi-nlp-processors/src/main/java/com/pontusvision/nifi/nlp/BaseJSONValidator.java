package com.pontusvision.nifi.nlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class BaseJSONValidator implements Validator
{

  Set<String> dataModels = new HashSet<>();

  @Override public ValidationResult validate(String subject, String input, ValidationContext context)
  {
    if (dataModels.isEmpty())
    {
      return validateJSONURLs(subject, input, context);
    }

    else
    {
      return VALID.validate(subject, input, context);
    }

  }

  public static ValidationResult validateJSONURLs(String subject, String input, ValidationContext context)
  {
    boolean valid     = true;
    String  error     = "";
    String  uriStr    = "";
    String  modelType = "";

    try
    {

      HashMap<String, String> result = new ObjectMapper().readValue(input, HashMap.class);

      Iterator<Map.Entry<String, String>> it = result.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<String, String> pair = it.next();

        modelType = pair.getKey();
        uriStr = pair.getValue();

        URL         url    = new URL(uriStr);
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

}
