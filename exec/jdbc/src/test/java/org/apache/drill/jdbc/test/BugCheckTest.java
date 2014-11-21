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

public class BugCheckTest extends JdbcTestQueryBase {

  @Rule
  public TestRule TIMEOUT = TestTools.getTimeoutRule(Integer.MAX_VALUE);


  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BugCheckTest.class);

  // TODO(f:dbarclay):TODO1062xx:  Fix: HACK: Disable Jetty status(?) server so unit tests
  // run (without Maven setup).
  @BeforeClass
  public static void setUpClass() {
    System.setProperty( "drill.exec.http.enabled", "false" );
  }

  @Test
  /*@Ignore( "Revisit re 'The current reader doesn't support getting next "
           + "information.' (because of '[0]'?)" )*/
  public void explore_2() throws Exception {
    JdbcAssert
        .withNoDefaultSchema()
        .sql( "SELECT kvgen(col[0]) FROM cp.`bug1641_single_map_case.json`" )
        .returns( "???=???" );
  }


 

}
