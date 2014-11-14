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
package org.apache.drill.jdbc.test;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Function;

public class TempBug1Test extends JdbcTestQueryBase {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JdbcNullOrderingAndGroupingTest.class);

  // TODO(f:dbarclay):TODO1062xx:  Fix: HACK: Disable Jetty status(?) server so unit tests
  // run (without Maven setup).
  @BeforeClass
  public static void setUpClass() {
    System.setProperty( "drill.exec.http.enabled", "false" );
  }

  @Test
  public void testBug() throws Exception {
    JdbcAssert
        .withNoDefaultSchema()
        .sql( "SELECT tbl.id, \n" +
              "       CAST( tbl.for_Decimal5 AS DECIMAL(5) ) AS as_DECIMAL5 \n" +
              "FROM cp.`null_ordering_and_grouping_data.json` AS tbl \n" +
              "ORDER BY no_such_column ASC NULLS LAST" )
        .returns( "Should have been SQL error about no such no_such_column!" );
  }


}
