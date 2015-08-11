package org.apache.drill.jdbc.proxy;
//import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.proxy.TracingProxyDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;


public class TestTracingProxyUsableViaZooKeeper {

  @Test
  public void testProxyUsable() throws SQLException {
    //Driver.load();
    new TracingProxyDriver();
    try (
      Connection conn = 
          DriverManager.getConnection(
              //"jdbc:proxy:org.apache.drill.jdbc.Driver:jdbc:drill:zk=1234"
              //"jdbc:proxy:org.apache.drill.jdbc.Driver:jdbc:drill:zk=10.10.71.14:2181"
              //"jdbc:proxy:org.apache.drill.jdbc.Driver:jdbc:drill:zk=10.10.71.14:5181"
              //"jdbc:proxy:org.apache.drill.jdbc.Driver:jdbc:drill:zk=10.10.71.14:5181/drill/my_cluster_com-drillbits"
              "jdbc:proxy:org.apache.drill.jdbc.Driver:jdbc:drill:zk=node3.mapr.com:5181/drill/my_cluster_com-drillbits"
              )
             ) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery( "VALUES 'Some string'" );
      assertThat( rs.next(), equalTo( true ) );
      String result = rs.getString( 1 );
      assertThat( result, equalTo( "Some string" ) );
    }
  }

}
