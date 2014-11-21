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

import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class CsvToAvroTest {
  
  public CsvToAvroTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  @Test
  public void testDefaultsNoHeader() throws Exception {
    String recordName = "default";
    String delimitedText = "joey,1.0,5,thirty";

    CsvToAvro csv = new CsvToAvro.Builder().build();
    GenericRecord result = csv.toAvro(recordName, delimitedText);
    assertEquals("joey", result.get("field_0"));
    assertEquals(1.0d, result.get("field_1"));
    assertEquals(5l, result.get("field_2"));
    assertEquals("thirty", result.get("field_3"));
  }

  @Test
  public void testDefaultsHeader() throws Exception {
    String recordName = "default";
    String delimitedText = "joey,1.0,5,thirty";
    String header = "name,score,count,num";
    
    CsvToAvro csv = new CsvToAvro.Builder().build();
    GenericRecord result = csv.toAvro(recordName, delimitedText, header);
    assertEquals("joey", result.get("name"));
    assertEquals(1.0d, result.get("score"));
    assertEquals(5l, result.get("count"));
    assertEquals("thirty", result.get("num"));
  }
  
}
