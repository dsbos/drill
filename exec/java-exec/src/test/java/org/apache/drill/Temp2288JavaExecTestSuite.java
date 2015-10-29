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
package org.apache.drill;

import org.apache.drill.exec.TestWindowFunctions;
import org.apache.drill.exec.fn.impl.TestAggregateFunction;
import org.apache.drill.exec.fn.impl.TestAggregateFunctions;
import org.apache.drill.exec.fn.impl.TestNewAggregateFunctions;
import org.apache.drill.exec.nested.TestNestedComplexSchema;
import org.apache.drill.exec.physical.impl.filter.TestLargeInClause;
import org.apache.drill.exec.physical.impl.flatten.TestFlatten;
import org.apache.drill.exec.physical.impl.join.TestMergeJoinAdvanced;
import org.apache.drill.exec.physical.impl.join.TestNestedLoopJoin;
import org.apache.drill.exec.physical.impl.window.TestWindowFrame;
import org.apache.drill.exec.store.json.TestDrill2288Related;
import org.apache.drill.exec.store.json.TestJsonRecordReader;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  TestDrill2288Related.class,
  TestUnionAll.class,

  TestUnionDistinct.class,
  TestExampleQueries.class,
  TestCorrelation.class,
  TestTpchDistributedStreaming.class,
  TestBugFixes.class,
  org.apache.drill.exec.fn.impl.TestCastFunctions.class,
  org.apache.drill.exec.physical.impl.TestCastFunctions.class,
  TestNewAggregateFunctions.class,
  TestAggregateFunction.class,
  TestAggregateFunctions.class,
  TestNestedComplexSchema.class,
  TestWindowFunctions.class,
  TestWindowFrame.class,
  TestJsonRecordReader.class,
  TestMergeJoinAdvanced.class,
  TestNestedLoopJoin.class,
  TestFlatten.class,
  TestLargeInClause.class
})
public class Temp2288JavaExecTestSuite {

}
