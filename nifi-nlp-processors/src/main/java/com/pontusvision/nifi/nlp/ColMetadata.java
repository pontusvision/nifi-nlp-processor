package com.pontusvision.nifi.nlp;

import java.util.ArrayList;
import java.util.List;

public class ColMetadata
{

  private String colName;
  private String primaryKeyName;
  private String foreignKeyName;
  private String typeName;
  private String colRemarks;
  private String isAutoIncr;
  private String isGenerated;
  private long   octetLen;
  private long   ordinalPos;
  private String defVal;
  private long   colSize;

  List<String> vals = new ArrayList<>();

  // Getter Methods

  public String getColName()
  {
    return colName;
  }

  public String getPrimaryKeyName()
  {
    return primaryKeyName;
  }

  public String getForeignKeyName()
  {
    return foreignKeyName;
  }

  public String getTypeName()
  {
    return typeName;
  }

  public String getColRemarks()
  {
    return colRemarks;
  }

  public String getIsAutoIncr()
  {
    return isAutoIncr;
  }

  public String getIsGenerated()
  {
    return isGenerated;
  }

  public long getOctetLen()
  {
    return octetLen;
  }

  public long getOrdinalPos()
  {
    return ordinalPos;
  }

  public String getDefVal()
  {
    return defVal;
  }

  public long getColSize()
  {
    return colSize;
  }

  // Setter Methods

  public void setColName(String colName)
  {
    this.colName = colName;
  }

  public void setPrimaryKeyName(String primaryKeyName)
  {
    this.primaryKeyName = primaryKeyName;
  }

  public void setForeignKeyName(String foreignKeyName)
  {
    this.foreignKeyName = foreignKeyName;
  }

  public void setTypeName(String typeName)
  {
    this.typeName = typeName;
  }

  public void setColRemarks(String colRemarks)
  {
    this.colRemarks = colRemarks;
  }

  public void setIsAutoIncr(String isAutoIncr)
  {
    this.isAutoIncr = isAutoIncr;
  }

  public void setIsGenerated(String isGenerated)
  {
    this.isGenerated = isGenerated;
  }

  public void setOctetLen(long octetLen)
  {
    this.octetLen = octetLen;
  }

  public void setOrdinalPos(long ordinalPos)
  {
    this.ordinalPos = ordinalPos;
  }

  public void setDefVal(String defVal)
  {
    this.defVal = defVal;
  }

  public void setColSize(long colSize)
  {
    this.colSize = colSize;
  }
  public List<String> getVals()
  {
    return vals;
  }

  public void setVals(List<String> vals)
  {
    this.vals = vals;
  }

}