/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.blocks;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;

public class CsvToAvro {

  private final String delimiter;
  private final String escape;
  private final String quote;
  private final String charsetName;

  private CsvToAvro(String delimiter, String escape, String quote, String charsetName) {
    this.delimiter = delimiter;
    this.escape = escape;
    this.quote = quote;
    this.charsetName = charsetName;
  }

  public GenericRecord toAvro(String recordName, String delimitedText) throws IOException {
    return toAvro(recordName, delimitedText, null);
  }

  public GenericRecord toAvro(String recordName, String delimitedText, String header) throws IOException {
    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .hasHeader(header != null)
        .charset(charsetName)
        .build();

    Schema schema = getSchema(recordName, delimitedText, header, props);

    ByteArrayInputStream input
        = new ByteArrayInputStream(delimitedText.getBytes(charsetName));
    GenericRecord record = new GenericData.Record(schema);
    CSVReader reader = CSVUtil.newReader(input, props);
    String[] next = reader.readNext();
    fillIndexed(record, next);

    return record;
  }

  private Schema getSchema(String recordName, String delimitedText, String header, CSVProperties props) throws IOException {

    Schema schema = get(recordName);
    if (schema == null) {

      StringBuilder data = new StringBuilder();
      if (header != null) {
        data.append(header).append('\n');
      }
      data.append(delimitedText);

      ByteArrayInputStream input
          = new ByteArrayInputStream(data.toString().getBytes(charsetName));

      schema = CSVUtil.inferNullableSchema(recordName, input, props);
      add(recordName, schema);
    }

    return schema;
  }

  private static void fillIndexed(IndexedRecord record, String[] data) {
    Schema schema = record.getSchema();
    for (int i = 0, n = schema.getFields().size(); i < n; i += 1) {
      final Schema.Field field = schema.getFields().get(i);
      if (i < data.length) {
        record.put(i, makeValue(data[i], field));
      } else {
        record.put(i, makeValue(null, field));
      }
    }
  }

  private static Object makeValue(String string, Schema.Field field) {
    Object value = makeValue(string, field.schema());
    if (value != null || nullOk(field.schema())) {
      return value;
    } else {
      // this will fail if there is no default value
      return ReflectData.get().getDefaultValue(field);
    }
  }

  /**
   * Returns whether null is allowed by the schema.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  private static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns a the value as the first matching schema type or null.
   *
   * Note that if the value may be null even if the schema does not allow the
   * value to be null.
   *
   * @param string a String representation of the value
   * @param schema a Schema
   * @return the string coerced to the correct type from the schema or null
   */
  private static Object makeValue(String string, Schema schema) {
    if (string == null) {
      return null;
    }

    try {
      switch (schema.getType()) {
        case BOOLEAN:
          return Boolean.valueOf(string);
        case STRING:
          return string;
        case FLOAT:
          return Float.valueOf(string);
        case DOUBLE:
          return Double.valueOf(string);
        case INT:
          return Integer.valueOf(string);
        case LONG:
          return Long.valueOf(string);
        case ENUM:
          // TODO: translate to enum class
          if (schema.hasEnumSymbol(string)) {
            return string;
          } else {
            try {
              return schema.getEnumSymbols().get(Integer.valueOf(string));
            } catch (IndexOutOfBoundsException ex) {
              return null;
            }
          }
        case UNION:
          Object value = null;
          for (Schema possible : schema.getTypes()) {
            value = makeValue(string, possible);
            if (value != null) {
              return value;
            }
          }
          return null;
        case NULL:
          return null;
        default:
          // FIXED, BYTES, MAP, ARRAY, RECORD are not supported
          throw new RuntimeException(
              "Unsupported field type:" + schema.getType());
      }
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  private final Map<String, Schema> schemaCache = Maps.newHashMap();

  private Schema get(String header) {
    synchronized (schemaCache) {
      return schemaCache.get(header);
    }
  }

  private void add(String header, Schema schema) {
    synchronized (schemaCache) {
      schemaCache.put(header, schema);
    }
  }

  public static class Builder {

    private String delimiter = ",";
    private String escape = "\\";
    private String quote = "\"";
    private String charsetName = Charset.defaultCharset().name();

    public Builder delimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder escape(String escape) {
      this.escape = escape;
      return this;
    }

    public Builder quote(String quote) {
      this.quote = quote;
      return this;
    }

    public Builder charsetName(String charsetName) {
      this.charsetName = charsetName;
      return this;
    }

    public CsvToAvro build() {
      return new CsvToAvro(delimiter, escape, quote, charsetName);
    }
  }

}
