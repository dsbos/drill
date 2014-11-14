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

import org.apache.drill.common.util.TestTools;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Function;

public class TempBug2Test extends JdbcTestQueryBase {

  @Rule
  public TestRule TIMEOUT = TestTools.getTimeoutRule(Integer.MAX_VALUE);


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
    for ( int i = 0; i < 100 ; i++ ) {
      System.out.println( "i = " + i );
      JdbcAssert
          .withNoDefaultSchema()
          .sql( "SELECT 1 AS x \n" +
                "FROM cp.`donuts.json` AS tbl\n" +
                "LIMIT 1" )
          .returns( "x=1" );
    }
  }


}
