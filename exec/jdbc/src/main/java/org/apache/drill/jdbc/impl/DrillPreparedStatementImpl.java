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
package org.apache.drill.jdbc.impl;

import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.util.Calendar;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;

/**
 * Implementation of {@link java.sql.PreparedStatement} for Drill.
 *
 * <p>
 * This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs; it is
 * instantiated using
 * {@link net.hydromatic.avatica.AvaticaFactory#newPreparedStatement}.
 * </p>
 */
abstract class DrillPreparedStatementImpl extends AvaticaPreparedStatement
    implements DrillPreparedStatement,
               DrillRemoteStatement {

  protected DrillPreparedStatementImpl(DrillConnectionImpl connection,
                                       AvaticaPrepareResult prepareResult,
                                       int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    super(connection, prepareResult,
          resultSetType, resultSetConcurrency, resultSetHoldability);
    connection.openStatementsRegistry.addStatement(this);
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this PreparedStatement is closed.
   *
   * @throws  AlreadyClosedSqlException  if PreparedStatement is closed
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "PreparedStatement is already closed." );
    }
  }


  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  @Override
  public DrillConnectionImpl getConnection() {
    // Can't throw any SQLException because AvaticaConnection's getConnection() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch ( AlreadyClosedSqlException e ) {
      throw new RuntimeException( e.getMessage(), e );
    }
    return (DrillConnectionImpl) super.getConnection();
  }

  @Override
  protected AvaticaParameter getParameter(int param) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Prepared-statement dynamic parameters are not supported.");
  }

  @Override
  public void cleanUp() {
    final DrillConnectionImpl connection1 = (DrillConnectionImpl) connection;
    connection1.openStatementsRegistry.removeStatement(this);
  }

  // Note:  Methods are in same order as in java.sql.PreparedStatement.


  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throwIfClosed();
    return super.executeQuery(sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throwIfClosed();
    return super.executeUpdate(sql);
  }

  // No close() (it doesn't throw SQLException if already closed).

  @Override
  public int getMaxFieldSize() throws SQLException {
    throwIfClosed();
    return super.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throwIfClosed();
    super.setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() {
    // Can't throw any SQLException because AvaticaConnection's getMaxRows() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch ( AlreadyClosedSqlException e ) {
      throw new RuntimeException( e.getMessage(), e );
    }
    return super.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throwIfClosed();
    super.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throwIfClosed();
    super.setEscapeProcessing(enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throwIfClosed();
    return super.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throwIfClosed();
    super.setQueryTimeout(seconds);
  }

  @Override
  public void cancel() throws SQLException {
    throwIfClosed();
    super.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwIfClosed();
    return super.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwIfClosed();
    super.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throwIfClosed();
    super.setCursorName(name);
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throwIfClosed();
    return super.execute(sql);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throwIfClosed();
    return super.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throwIfClosed();
    return super.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throwIfClosed();
    return super.getMoreResults();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throwIfClosed();
    super.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection(){
    // Can't throw any SQLException because AvaticaConnection's getXXX() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch ( AlreadyClosedSqlException e ) {
      throw new RuntimeException( e.getMessage(), e );
    }
    return super.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throwIfClosed();
    super.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() {
    // Can't throw any SQLException because AvaticaConnection's getXXX() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch ( AlreadyClosedSqlException e ) {
      throw new RuntimeException( e.getMessage(), e );
    }
    return super.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throwIfClosed();
    return super.getResultSetConcurrency();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throwIfClosed();
    return super.getResultSetType();
  }

  @Override
  public void addBatch( String sql ) throws SQLException {
    throwIfClosed();
    super.addBatch( sql );
  }

  @Override
  public void clearBatch() throws SQLException {
    throwIfClosed();
    super.clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throwIfClosed();
    return super.executeBatch();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throwIfClosed();
    return super.getMoreResults(current);
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throwIfClosed();
    return super.getGeneratedKeys();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    return super.executeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throwIfClosed();
    return super.executeUpdate(sql, columnIndexes);
  }

  @Override
  public int executeUpdate(String sql, String columnNames[]) throws SQLException {
    throwIfClosed();
    return super.executeUpdate(sql, columnNames);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    return super.execute(sql, autoGeneratedKeys);
  }

  @Override
  public boolean execute(String sql, int columnIndexes[]) throws SQLException {
    throwIfClosed();
    return super.execute(sql, columnIndexes);
  }

  @Override
  public boolean execute(String sql, String columnNames[]) throws SQLException {
    throwIfClosed();
    return super.execute(sql, columnNames);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throwIfClosed();
    return super.getResultSetHoldability();
  }

  @Override
  public boolean isClosed() {
    try {
      return super.isClosed();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Unexpected " + e + " from AvaticaPreparedStatement.isClosed" );
    }
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throwIfClosed();
    super.setPoolable(poolable);
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throwIfClosed();
    return super.isPoolable();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throwIfClosed();
    super.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throwIfClosed();
    return super.isCloseOnCompletion();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    throwIfClosed();
    return super.executeQuery();
  }

  @Override
  public int executeUpdate() throws SQLException {
    throwIfClosed();
    return super.executeUpdate();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throwIfClosed();
    super.setNull( parameterIndex, sqlType);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    throwIfClosed();
    super.setBoolean( parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throwIfClosed();
    super.setByte(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    throwIfClosed();
    super.setShort(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    throwIfClosed();
    super.setInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    throwIfClosed();
    super.setLong(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    throwIfClosed();
    super.setFloat(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    throwIfClosed();
    super.setDouble(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throwIfClosed();
    super.setBigDecimal(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    throwIfClosed();
    super.setString(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte x[]) throws SQLException {
    throwIfClosed();
    super.setBytes(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
    throwIfClosed();
    super.setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
    throwIfClosed();
    super.setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
    throwIfClosed();
    super.setTimestamp(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
    throwIfClosed();
    super.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
    throwIfClosed();
    super.setUnicodeStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
    throwIfClosed();
    super.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void clearParameters() throws SQLException {
    throwIfClosed();
    super.clearParameters();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throwIfClosed();
    super.setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    throwIfClosed();
    super.setObject(parameterIndex, x);
  }

  @Override
  public boolean execute() throws SQLException {
    throwIfClosed();
    return super.execute();
  }

  @Override
  public void addBatch() throws SQLException {
    throwIfClosed();
    super.addBatch();
  }

  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException {
    throwIfClosed();
    super.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throwIfClosed();
    super.setRef(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throwIfClosed();
    super.setBlob(parameterIndex, x);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throwIfClosed();
    super.setClob(parameterIndex, x);
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throwIfClosed();
    super.setArray(parameterIndex, x);
  }

  @Override
  public ResultSetMetaData getMetaData() {
    // Can't throw any SQLException because AvaticaConnection's getMetaData() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch ( AlreadyClosedSqlException e ) {
      throw new RuntimeException( e.getMessage(), e );
    }
    return super.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) throws SQLException {
    throwIfClosed();
    super.setDate(parameterIndex, x, cal);
  }

  @Override
  public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) throws SQLException {
    throwIfClosed();
    super.setTime(parameterIndex, x, cal);
  }

  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException {
    throwIfClosed();
    super.setTimestamp(parameterIndex, x, cal);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throwIfClosed();
    super.setNull(parameterIndex, sqlType, typeName);
  }

  @Override
  public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
    throwIfClosed();
    super.setURL(parameterIndex, x);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throwIfClosed();
    return super.getParameterMetaData();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setRowId(parameterIndex, x);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setNString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setNCharacterStream(parameterIndex, value, length);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setNClob(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setClob(parameterIndex, reader, length);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setBlob(parameterIndex, inputStream, length);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throwIfClosed();
    //??? abstract, see factory: super.setNClob(parameterIndex, reader, length);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setSQLXML(parameterIndex, xmlObject);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throwIfClosed();
    super.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setAsciiStream(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setBinaryStream(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setCharacterStream(parameterIndex, reader);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setNCharacterStream(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setClob(parameterIndex, reader);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setBlob(parameterIndex, inputStream);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throwIfClosed();
  //??? abstract, see factory: super.setNClob(parameterIndex, reader);
  }
}
