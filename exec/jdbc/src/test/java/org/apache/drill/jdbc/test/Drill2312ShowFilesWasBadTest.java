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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.drill.jdbc.Driver;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;


public class Drill2312ShowFilesWasBadTest {

  private static Connection connection;


  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Driver.load();
    connection = DriverManager.getConnection( "jdbc:drill:zk=local" );
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
  }


  @Test
  public void testNameThis() throws SQLException {
    Map<String, Boolean> isFileMap1 = new HashMap<>();
    Map<String, Boolean> isFileMap2 = new HashMap<>();
    Map<String, Boolean> isDirectoryMap = new HashMap<>();

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery( "USE dfs" );
    while( rs.next() ) {
    }

    for ( int iter = 1 ; iter <= 100; iter++ ) {
      System.err.println( "iter: = " + iter );

      rs = stmt.executeQuery( "SHOW FILES" );
      //ResultSetMetaData rsmd = rs.getMetaData();
      while ( rs.next() ) {
        System.err.println();
        /*
        System.err.println( "rs = " + rs );
        for ( int cx = 1; cx <= rsmd.getColumnCount(); cx++ ) {
          System.err.println( rsmd.getColumnLabel( cx ) + ": " + rs.getObject( cx ) );
        }
        */
        final String name = rs.getString( "name" );
        /*
        System.err.println( "name: " + rs.getObject( "name" ) );
        System.err.println( "isFile getObject(...): " + rs.getObject( "isFile" ) );
        System.err.println( "isFile getBoolean(...): " + rs.getBoolean( "isFile" ) );
        System.err.println( "isDirectory: " + rs.getObject( "isDirectory" ) );
        System.err.println( "isDirectory getBoolean(...): " + rs.getBoolean( "isDirectory" ) );
        */

        final Boolean isFile1 = isFileMap1.get( name );
        if ( null != isFile1 ) {
          assertEquals( isFile1, rs.getBoolean( "isFile" ) );
          assertEquals( isFile1, rs.getObject( "isFile" ) );
        }
        else {
          isFileMap1.put( name, rs.getBoolean( "isFile" ) );
        }

        final Boolean isFile2 = isFileMap2.get( name );
        if ( null != isFile2 ) {
          assertEquals( isFile2, rs.getBoolean( "isFile" ) );
          assertEquals( isFile2, rs.getObject( "isFile" ) );
        }
        else {
          isFileMap2.put( name, (Boolean) rs.getObject( "isFile" ) );
        }


      }

    }
  }



}
