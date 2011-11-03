/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.backup.check.DiffRecordStore;
import org.neo4j.backup.check.DiffStore;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DataInconsistencyError;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogApplier;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogDeserializer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * This class implements a transaction stream deserializer which verifies
 * the integrity of the store for each applied transaction. It is assumed that
 * the stream contains a defragmented serialized transaction.
 */
class VerifyingLogDeserializer implements LogDeserializer
{
    private final boolean rejectInconsistentTransactions;
    private final ReadableByteChannel byteChannel;
    private final LogBuffer writeBuffer;
    private final LogApplier applier;
    private final XaCommandFactory cf;
    private final ByteBuffer scratchBuffer = ByteBuffer.allocateDirect( 9
                                                                       + Xid.MAXGTRIDSIZE
                                                                       + Xid.MAXBQUALSIZE
                                                                       * 10 );

    private final List<LogEntry> logEntries;
    private final DiffStore diffs;
    private final StringLogger msgLog;

    private LogEntry.Commit commitEntry;
    private LogEntry.Start startEntry;

    VerifyingLogDeserializer( ReadableByteChannel byteChannel,
            LogBuffer writeBuffer, LogApplier applier, XaCommandFactory cf,
            NeoStoreXaDataSource ds, boolean rejectInconsistentTransactions )
    {
        this.byteChannel = byteChannel;
        this.writeBuffer = writeBuffer;
        this.applier = applier;
        this.cf = cf;
        this.rejectInconsistentTransactions = rejectInconsistentTransactions;
        this.logEntries = new LinkedList<LogEntry>();
        this.diffs = new DiffStore( ds.getNeoStore() );
        this.msgLog = ds.getMsgLog();
    }

    @Override
    public boolean readAndWriteAndApplyEntry( int newXidIdentifier )
            throws IOException
    {
        LogEntry entry = LogIoUtils.readEntry( scratchBuffer, byteChannel, cf );
        if ( entry == null )
        {
            /*
             * The following two cannot run on commit entry found - it is not
             * always present. Even if the stream is incomplete though,
             * XaLogicalLog does take that into account, so we can go ahead and apply.
             */
            verify();
            applyAll();
            return false;
        }
        entry.setIdentifier( newXidIdentifier );
        logEntries.add( entry );
        if ( entry instanceof LogEntry.Commit )
        {
            assert startEntry != null;
            commitEntry = (LogEntry.Commit) entry;
        }
        else if ( entry instanceof LogEntry.Start )
        {
            startEntry = (LogEntry.Start) entry;
        }
        else if ( entry instanceof LogEntry.Command )
        {
            XaCommand command = ( (LogEntry.Command) entry ).getXaCommand();
            if ( command instanceof Command )
            {
                ( (Command) command ).accept( diffs );
            }
        }
        return true;
    }

    @Override
    public Commit getCommitEntry()
    {
        return commitEntry;
    }

    @Override
    public Start getStartEntry()
    {
        return startEntry;
    }

    private void verify() throws DataInconsistencyError
    {
        /*
         *  Here goes the actual verification code. If it passes,
         *  just return - if not, throw Error so that the
         *  store remains safe.
         */
        ConsistencyCheck consistency = diffs.applyToAll( new ConsistencyCheck( diffs )
        {
            @Override
            protected <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record, String message )
            {
                StringBuilder log = messageHeader( "Inconsistencies" );
                logRecord( log, recordStore, record );
                log.append( message );
                msgLog.logMessage( log.toString() );
            }

            @Override
            protected <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    String message )
            {
                if ( recordStore == referredStore && record.getLongId() == referred.getLongId() )
                { // inconsistency between versions, logRecord() handles that, treat as single record
                    report( recordStore, record, message );
                    return;
                }
                StringBuilder log = messageHeader( "Inconsistencies" );
                logRecord( log, recordStore, record );
                logRecord( log, referredStore, referred );
                log.append( message );
                msgLog.logMessage( log.toString() );
            }
        } );
        try
        {
            consistency.checkResult();
        }
        catch ( AssertionError e )
        {
            DataInconsistencyError error = new DataInconsistencyError( "Cannot apply transaction\n\t" + startEntry + "\n\t"
                                                                       + commitEntry + "\n\t" + e.getMessage() );
            msgLog.logMessage( error.getMessage() );
            if ( rejectInconsistentTransactions )
            {
                startEntry = null;
                commitEntry = null;
                logEntries.clear();
                throw error;
            }
            else
            {
                final StringBuilder changes = messageHeader( "Changes" );
                diffs.applyToAll( new RecordStore.Processor()
                {
                    @Override
                    protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, RecordStore<R> store,
                            R record )
                    {
                        DiffRecordStore<R> diff = (DiffRecordStore<R>)store;
                        if (diff.isModified( record.getLongId() )) 
                        {
                            logRecord( changes, store, record );
                        }
                    }
                } );
                msgLog.logMessage( changes.toString() );
            }
        }
    }

    private static <R extends AbstractBaseRecord> void logRecord( StringBuilder log, RecordStore<? extends R> store,
            R record )
    {
        DiffRecordStore<? extends R> diff = (DiffRecordStore<? extends R>) store;
        if ( diff.isModified( record.getLongId() ) )
        {
            log.append( "- " ).append( diff.forceGetRaw( record.getLongId() ) ).append( "\n\t+ " );
        }
        log.append( record ).append( "\n\t" );
    }
    
    private StringBuilder messageHeader( String type )
    {
        StringBuilder log = new StringBuilder( type ).append( " in transaction" );
        if ( commitEntry != null )
            log.append( " (txId=" ).append( commitEntry.getTxId() ).append( ")" );
        else if ( startEntry != null )
            log.append( " (log local id = " ).append( startEntry.getIdentifier() ).append( ")" );
        return log.append( ":\n\t" );
    }

    private void applyAll() throws IOException
    {
        for ( LogEntry entry : logEntries )
        {
            LogIoUtils.writeLogEntry( entry, writeBuffer );
            applier.apply( entry );
        }
    }
}
