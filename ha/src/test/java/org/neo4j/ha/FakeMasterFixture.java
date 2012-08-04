package org.neo4j.ha;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.nioneo.xa.CommandAccessFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class FakeMasterFixture implements TestRule, FakeMaster.MasterDataProvider
{
    @SuppressWarnings( "deprecation" )
    static String NEO_DATA_SOURCE_NAME = Config.DEFAULT_DATA_SOURCE_NAME;
    private static final int MASTER_ID = 1, SLAVE_ID = 2;

    public static final DataInitializer EMPTY_INITIAL_DATABASE = new DataInitializer()
    {
        @Override
        public void initialize( GraphDatabaseService db )
        {
            // do nothing
        }
    };

    public HighlyAvailableGraphDatabase startDatabase( DataInitializer seed )
    {
        if ( database != null )
        {
            throw new IllegalStateException( "already started" );
        }

        // use a non-ha database to create some store files, because without them an HA database won't start
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( dir.getAbsolutePath(),
                                                              stringMap( keep_logical_logs.name(), "true" ) );

        seed.initialize( db );

        db.shutdown();

        initState();

        return this.database = new SlaveConnectedToFakeMaster();
    }

    public void internalRestart( boolean rotateLogs )
    {
        database.internalRestart( rotateLogs );
    }

    public interface DataInitializer
    {
        void initialize( GraphDatabaseService db );
    }

    public void inject( MasterTransaction tx )
    {
        transactions.put( ++this.txIdGenerator, tx );
    }

    public static abstract class MasterTransaction
    {
        protected abstract void transactionData( TransactionCommandFactory tx );

        private final long startTimestamp = System.currentTimeMillis();
        private final XidImpl xid = new XidImpl( XidImpl.getNewGlobalId(), NeoStoreXaDataSource.BRANCH_ID );

        private long checksum()
        {
            return ((long) (MASTER_ID * 37 + MASTER_ID) << 32) | ((long) xid.hashCode() & 0xFFFFFFFFL);
        }

        private Triplet<String, Long, TxExtractor> extract( int localId, long txId )
        {
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            // NOTE: code stolen from org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start.getChecksum()
            try
            {
                LogIoUtils.writeStart( buffer, localId, xid, MASTER_ID, MASTER_ID, startTimestamp );

                transactionData( new TransactionCommandFactory( buffer, localId ) );

                LogIoUtils.writeCommit( false, buffer, localId, txId, System.currentTimeMillis() );
                LogIoUtils.writeDone( buffer, localId );
            }
            catch ( IOException e )
            {
                throw new Error( "InMemoryLogBuffer should not throw exception", e );
            }
            return Triplet.of( NEO_DATA_SOURCE_NAME, txId, TxExtractor.create( buffer ) );
        }
    }

    public static class TransactionCommandFactory
    {
        private final LogBuffer buffer;
        private final int localId;

        private TransactionCommandFactory( LogBuffer buffer, int localId )
        {
            this.buffer = buffer;
            this.localId = localId;
        }

        public void propertyKey( int id, String key )
        {
            write( CommandAccessFactory.command( withName( new PropertyIndexRecord( id ), key ) ) );
        }

        public void relationshipType( int id, String label )
        {
            write( CommandAccessFactory.command( withName( new RelationshipTypeRecord( id ), label ) ) );
        }

        public void create( NodeRecord node )
        {
            node.setCreated();
            update( node );
        }

        public void update( NodeRecord node )
        {
            node.setInUse( true );
            add( node );
        }

        public void delete( NodeRecord node )
        {
            node.setInUse( false );
            add( node );
        }

        public void create( RelationshipRecord relationship )
        {
            relationship.setCreated();
            update( relationship );
        }

        public void update( RelationshipRecord relationship )
        {
            relationship.setInUse( true );
            add( relationship );
        }

        public void delete( RelationshipRecord relationship )
        {
            relationship.setInUse( false );
            add( relationship );
        }

        public void create( PropertyRecord property )
        {
            property.setCreated();
            update( property );
        }

        public void update( PropertyRecord property )
        {
            property.setInUse( true );
            add( property );
        }

        public void delete( PropertyRecord property )
        {
            property.setInUse( false );
            add( property );
        }

        private void add( NodeRecord node )
        {
            write( CommandAccessFactory.command( node ) );
        }

        private void add( RelationshipRecord relationship )
        {
            write( CommandAccessFactory.command( relationship ) );
        }

        private void add( PropertyRecord property )
        {
            write( CommandAccessFactory.command( property ) );
        }

        private void write( Command command )
        {
            try
            {
                LogIoUtils.writeCommand( buffer, localId, command );
            }
            catch ( IOException e )
            {
                throw new Error( "InMemoryLogBuffer should not throw exception", e );
            }
        }

        private static <T extends AbstractNameRecord> T withName( T record, String name )
        {
            record.setInUse( true );
            DynamicRecord valueRecord = new DynamicRecord( record.getId() );
            valueRecord.setInUse( true );
            record.setNameId( record.getId() );
            byte[] data = PropertyStore.encodeString( name );
            if ( data.length > AbstractNameStore.NAME_STORE_BLOCK_SIZE )
            {
                throw new IllegalArgumentException( "Name is too long to fit in a single block" );
            }
            valueRecord.setData( data );
            record.addNameRecord( valueRecord );
            return record;
        }
    }

    private File dir;
    private SlaveConnectedToFakeMaster database;
    private final Map<Long, MasterTransaction> transactions = new HashMap<Long, MasterTransaction>();
    private final Map<Long, Long> checksums = new HashMap<Long, Long>();
    private int localIdGenerator = 0;
    private long txIdGenerator = 1;
    private StoreId storeId;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        final TargetDirectory.TestDirectory dir = TargetDirectory.forTest( description.getTestClass() )
                                                                 .cleanTestDirectory();
        return dir.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                FakeMasterFixture.this.dir = dir.directory();
                try
                {
                    base.evaluate();
                }
                finally
                {
                    FakeMasterFixture.this.cleanup();
                }
            }
        }, description );
    }

    private void initState()
    {
        StoreAccess stores = new StoreAccess( dir.getAbsolutePath() );
        try
        {
            NeoStore neoStore = stores.getRawNeoStore();
            this.txIdGenerator = neoStore.getLastCommittedTx();
            this.storeId = neoStore.getStoreId();
        }
        finally
        {
            stores.close();
        }
        this.localIdGenerator = 0;

        try
        {
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            LogExtractor extractor = LogExtractor.from( dir.getAbsolutePath() );
            for ( long txId; -1 != (txId = extractor.extractNext( buffer )); )
            {
                checksums.put( txId, extractor.getLastTxChecksum() );
            }
        }
        catch ( IOException e )
        {
            return;
        }
    }

    private void cleanup()
    {
        if ( database != null )
        {
            database.shutdown();
        }
        this.database = null;
        this.transactions.clear();
        this.checksums.clear();

        this.storeId = null;
        this.dir = null;
    }

    private class SlaveConnectedToFakeMaster extends HighlyAvailableGraphDatabase
    {
        public SlaveConnectedToFakeMaster()
        {
            super( dir.getAbsolutePath(), stringMap( HaSettings.server_id.name(), "" + SLAVE_ID ) );
        }

        @Override
        protected Broker createBroker()
        {
            return new FakeSlaveBroker( new FakeMaster( FakeMasterFixture.this ), 1, this.configuration );
        }

        @Override
        protected ClusterClient createClusterClient()
        {
            return new FakeClusterClient( getBroker() );
        }

        void internalRestart( boolean rotateLogs )
        {
            internalShutdown( rotateLogs );
            reevaluateMyself();
        }
    }

    @Override
    public Triplet<String, Long, TxExtractor> transaction( long txId )
    {
        MasterTransaction transaction = transactions.get( txId );
        if ( transaction == null )
        {
            return null;
        }
        return transaction.extract( localIdGenerator++, txId );
    }

    @Override
    public int id()
    {
        return MASTER_ID;
    }

    @Override
    public long transactionChecksum( long txId )
    {
        MasterTransaction transaction = transactions.get( txId );
        Long checksum;
        if ( transaction != null )
        {
            checksum = transaction.checksum();
        }
        else
        {
            checksum = checksums.get( txId );
        }
        return checksum == null ? 0 : checksum;
    }

    @Override
    public StoreId storeId()
    {
        return storeId;
    }
}
