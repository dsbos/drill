/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.json;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDrill2288Related extends BaseTestQuery {

  @BeforeClass
  public static void setUpClass() throws Exception {
   // test( "alter session set `planner.slice_target` = 1" );
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    // test( "alter session reset `planner.slice_target`" );
  }

  @Test
  public void testNameThisA_SameEmptySame() throws Exception {
    String dir =
        FileUtils.getResourceAsFile("/store/json/drill2288SameEmptySame").toURI().toString();

    test( "SELECT CAST(t.onecf.subColA AS varchar(30)) name, COUNT(*) unique_name "
          + "FROM dfs_test.`" + dir + "` AS t "
          + "GROUP BY t.onecf.subColA "
          + "HAVING COUNT(*) > 3 "
          + "ORDER BY t.onecf.subColA" );
  }

  @Test
  public void testNameThisB_SimSimEmptySim() throws Exception {
    String dir =
        FileUtils.getResourceAsFile("/store/json/drill2288SimSimEmptySim").toURI().toString();

    test( "SELECT CAST(t.onecf.subColA AS varchar(30)) name, COUNT(*) unique_name "
          + "FROM dfs_test.`" + dir + "` AS t "
          + "GROUP BY t.onecf.subColA "
          + "HAVING COUNT(*) > 3 "
          + "ORDER BY t.onecf.subColA" );
  }

  @Test
  public void testNameThisB_SimSimEmptySim_Star() throws Exception {
    String dir =
        FileUtils.getResourceAsFile("/store/json/drill2288SimSimEmptySim").toURI().toString();

    test( "SELECT * FROM dfs_test.`" + dir + "`" );
  }

  @Test
  public void testNameThis11() throws Exception {
    try {
      test( "alter session set `planner.slice_target` = 1" );
      String dir =
          FileUtils.getResourceAsFile("/store/json/drill-2288_empty_between_same_schema")
              .toURI().toString();

      test( "SELECT CAST(t.onecf.subColA AS varchar(30)) subColA, COUNT(*) unique_name "
            + "FROM dfs_test.`" + dir + "` AS t "
            + "GROUP BY t.onecf.subColA "
            + "HAVING COUNT(*) > 3 "
            + "ORDER BY t.onecf.subColA" );
    } finally {
      test( "alter session reset `planner.slice_target`" );
    }
  }

  @Test
  public void testNameThis13B() throws Exception {
    try {
      String dir =
          FileUtils.getResourceAsFile("/store/json/drill2288SimSimEmptySim").toURI().toString();

      test( "SELECT CAST(t.onecf.subColA AS varchar(30)) name, COUNT(*) unique_name "
            + "FROM dfs_test.`" + dir + "` AS t "
            + "GROUP BY t.onecf.subColA "
            + "HAVING COUNT(*) > 3 "
            + "ORDER BY t.onecf.subColA" );
    } finally {
    }
  }

  @Test
  public void testNameThis14() throws Exception {

    try {
      test( "alter session set `planner.slice_target` = 1" );

      String dir =
          FileUtils.getResourceAsFile("/store/json/drill2288namethis2/voter3.json").toURI().toString();

      test( "SELECT CAST(t.onecf.subColA AS varchar(30)) name, COUNT(*) unique_name "
            + "FROM dfs_test.`" + dir + "` AS t "
            + "GROUP BY t.onecf.subColA "
            + "HAVING COUNT(*) > 3 "
            + "ORDER BY t.onecf.subColA" );
    } finally {
      test( "alter session reset `planner.slice_target`" );
    }
  }

}
