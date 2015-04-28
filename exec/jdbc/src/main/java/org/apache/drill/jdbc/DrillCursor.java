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
package org.apache.drill.jdbc;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import net.hydromatic.avatica.ArrayImpl.Factory;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.Cursor;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.apache.drill.jdbc.impl.DrillResultSetImpl;

public class DrillCursor implements Cursor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCursor.class);

  /** Value for catalog, schema, or table name when not applicable. */
  private static final String UNKNOWN_NAME_VALUE = "";

  /** The associated java.sql.ResultSet implementation. */
  private final DrillResultSetImpl resultSet;


  /** resultSet's RecordBatchLoader; holds any current batch */
  private final RecordBatchLoader currentBatchHolder;
  /** resultSet's ResultsListener; for getting {@link QueryDataBatch}es */
  private final DrillResultSetImpl.ResultsListener resultsListener;

  /** Whether next() has been called; TODO: for what? */
  private boolean started = false;
  private boolean finished = false;
  // TODO:  Doc.: Say what "readFirstNext" means.  ?? Something to do with zero-row batch
  private boolean redoFirstNextxxx = false;
  // TODO:  Doc.: First what? (First batch? record? "next" call/operation?)
  private boolean firstWHATxxx = true;
  private String tempReturnedFalseCase = "";

  /** Zero-based index (offset) of current record in record batch; -1 before
   *  first used. */
  private int currentRecordNumber = -1;

  /** Number of record batches read from results listener, including null read at end(??). */
  private long recordBatchCount;

  /** Drill schema of current batch; null before first used. */
  private BatchSchema schema;

  /** ~JDBC schema of current batch; null before first used. */
  private DrillColumnMetaDataList columnMetaDataList;

  /** Column data accessors, configured for current batch's schema (except
   *  before first batch). */
  private final DrillAccessorList accessors = new DrillAccessorList();


  /**
   *
   * @param  resultSet  the associated ResultSet implementation
   */
  public DrillCursor(final DrillResultSetImpl resultSet) {
    this.resultSet = resultSet;
    currentBatchHolder = resultSet.currentBatch;
    resultsListener = resultSet.resultsListener;
  }

  public DrillResultSetImpl getResultSet() {
    return resultSet;
  }

  protected int getCurrentRecordNumber() {
    return currentRecordNumber;
  }

  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types, Calendar localCalendar, Factory factory) {
    columnMetaDataList = (DrillColumnMetaDataList) types;
    return accessors;
  }

  // TODO:  Doc.:  Specify what the return value actually means.  (The wording
  // "Moves to the next row" and "Whether moved" from the documentation of the
  // implemented interface (net.hydromatic.avatica.Cursor) doesn't address
  // moving past last row or how to evaluate "whether moved" on the first call.
  // In particular, document what the return value indicates about whether we're
  // currently at a valid row (or whether next() can be called again, or
  // whatever it does indicate), especially the first time this next() called
  // for a new result.
  @Override
  public boolean next() throws SQLException {
    logger.debug( "next() (entry): started = {}, firstWHAT = {}, redoFirstNext = {},"
                  + " finished = {}, currentRecordNumber = {}.",
                  started, firstWHATxxx, redoFirstNextxxx, finished, currentRecordNumber );
    if ( null != tempReturnedFalseCase ) {
      logger.info( "NOTE:  tempReturnedFalseCase = " + tempReturnedFalseCase );
    }
    if (!started) {
      // is first call to next() (from DrillResultSetImpl.execute())
      started = true;
      redoFirstNextxxx = true; // ???? Why exactly are we setting this true here rather than in initialization?  (For clarity?)
    } else if (redoFirstNextxxx && !finished) {
      // is subsequent call to next() (from AvaticaResultSet.next()),
      // and <what the heck does "redoFirstNext" mean?> (was set true by first call and not set yet false here or one place below?),
      // and we're not already finished (early?)
      redoFirstNextxxx = false;
      logger.debug( "next(): exit #r1: return true: (what case is this?)" ); //???? testDril2503
      return true;
    }

    // TODO: CONFIRM:  Why not do this first, so it's clear that it doesn't
    // depend (directly) on any other state bits?
    if (finished) {
      logger.debug( "next(): exit #r2: return false: already finished (hit end in previous call)" );
      tempReturnedFalseCase += " + already finished";
      return false;  // ???? Q: Does this mean next() shouldn't be called any more?  (ResultSet.next() can be, but what about this?)
    }

    if (currentRecordNumber + 1 < currentBatchHolder.getRecordCount()) {
      // Next index is in within current batch--just increment to that record.
      currentRecordNumber++;
      logger.debug( "next(): exit #r3: advanced to next record in current batch" );
      return true;
    } else {
      // Next index is not in current batch (including initial empty batch--
      // (try to) get next batch.
      try {
        QueryDataBatch qrb = resultsListener.getNext();
        recordBatchCount++;
        // ?????? Why is firstWhatxxx inside the while condition rather than
        // before it?  (It can't change inside the while statement.)
        while (qrb != null  // there is a batch,
               && (qrb.getHeader().getRowCount() == 0 || qrb.getData() == null )
                            // the batch had no row or no <what kind of> data, and
               && ! firstWHATxxx   // ?????? first what, exactly?  first batch?
                                   // first batch with some condition? -
                                   // (first batch that didn't return just below)
               ) {
          qrb.release();
          qrb = resultsListener.getNext();  // get next batch
          recordBatchCount++;
          if (qrb != null && qrb.getData() == null) {  // if no data--indicating what????
            qrb.release();
            logger.debug( "next(): exit #r4: return false: ... (what case is this?)" );
            tempReturnedFalseCase += "null getData()? (4)";
            return false;  // ???? Q: Does this mean next() shouldn't be called any more?  (ResultSet.next() can be, but what about this?)
          }
        }

        // TODO:  Resolve and rename/doc.:  First WHAT?!
        //   (Not first call to next(), not first QueryDataBatch (if can be null);
        //   What?)
        //  Controls only while statement above
        firstWHATxxx = false;

        if (qrb == null) {
          // If no more batches(?), then done.
          currentBatchHolder.clear();
          finished = true;
          logger.debug( "next(): exit #r5: return false: just got null getting next QueryDataBatch" );
          tempReturnedFalseCase += "some kind of null QueryDataBase";
          return false;  // ???? Q: Does this mean next() shouldn't be called any more?  (ResultSet.next() can be, but what about this?)
        } else {
          // Got a new batch.
          currentRecordNumber = 0;
          final boolean schemaChanged;
          try {
            schemaChanged = currentBatchHolder.load(qrb.getHeader().getDef(), qrb.getData());
          }
          finally {
            qrb.release();
          }
          schema = currentBatchHolder.getSchema();
          if (schemaChanged) {
            updateColumns();
          }
          if (redoFirstNextxxx && currentBatchHolder.getRecordCount() == 0) {
            // ?????: have had whatever firstWHAT refers to, and got
            // a batch with zero records--what does that mean, and what does
            // this control?  Controls something about quickly returning "true"
            redoFirstNextxxx = false;
          }
          logger.debug( "next(): exit #r6: return true: (what case is this?  got QueryDataBatch, with ??what?)" );
          return true;
        }
      }
      catch ( UserException e ) {
        // A normally expected case--for any server-side error (e.g., syntax
        // error in SQL statement).
        // Contruct SQLException with message text from the UserException.
        // TODO:  Map UserException error type to SQLException subclass (once
        // error type is accessible, of course. :-( )
        throw new SQLException( e.getMessage(), e );
      }
      catch ( InterruptedException e ) {
        // Not normally expected--Drill doesn't interrupt in this area (right?)--
        // but JDBC client certainly could.
        throw new SQLException( "Interrupted.", e );
      }
      catch ( SchemaChangeException e ) {
        // TODO:  Clean:  DRILL-2933:  RecordBatchLoader.load(...) no longer
        // throws SchemaChangeException, so check/clean catch clause.
        throw new SQLException(
            "Unexpected SchemaChangeException from RecordBatchLoader.load(...)" );
      }
      catch ( RuntimeException e ) {
        throw new SQLException( "Unexpected exception: " + e.toString(), e );
      }

    }
  }

  void updateColumns() {
    columnMetaDataList.updateColumnMetaData(
        InfoSchemaConstants.IS_CATALOG_NAME, UNKNOWN_NAME_VALUE /* schema */,
        UNKNOWN_NAME_VALUE /* table */, schema);
    accessors.generateAccessors(this, currentBatchHolder);
    if (getResultSet().changeListener != null) {
      getResultSet().changeListener.schemaChanged(schema);
    }
  }

  // ???? Is this used?
  public long getRecordBatchCount() {
    return recordBatchCount;
  }

  @Override
  public void close() {
    // currentBatch is owned by resultSet and cleaned up by
    // DrillResultSetImpl.cleanup()

    // listener is owned by resultSet and cleaned up by
    // DrillResultSetImpl.cleanup()

    // Clean up result set (to deallocate any buffers).
    getResultSet().cleanup();
    // TODO:  CHECK:  Something might need to set statement.openResultSet to
    // null.  Also, AvaticaResultSet.close() doesn't check whether already
    // closed and skip calls to cursor.close(), statement.onResultSetClose()
  }

  // TODO:  Resolve:  What is this for?  Is doesn't seem to be called ever.
  @Override
  public boolean wasNull() throws SQLException {
    return accessors.wasNull();
  }

}
