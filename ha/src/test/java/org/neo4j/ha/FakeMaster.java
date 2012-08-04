package org.neo4j.ha;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

import static org.neo4j.ha.FakeMasterFixture.NEO_DATA_SOURCE_NAME;

class FakeMaster implements Master
{
    interface MasterDataProvider
    {
        Triplet<String,Long,TxExtractor> transaction( long txId );

        int id();

        long transactionChecksum( long txId );

        StoreId storeId();
    }

    private final MasterDataProvider provider;

    public FakeMaster( MasterDataProvider provider )
    {
        this.provider = provider;
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return new Response<Void>( null, provider.storeId(), new SlaveTransactionStream( context ),
                                   ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId myStoreId )
    {
        return new Response<Pair<Integer, Long>>( Pair.of( provider.id(), provider.transactionChecksum( txId ) ),
                                                  provider.storeId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    class SlaveTransactionStream extends TransactionStream
    {
        private long txId;

        SlaveTransactionStream( RequestContext context )
        {
            super( NEO_DATA_SOURCE_NAME );
            for ( RequestContext.Tx tx : context.lastAppliedTransactions() )
            {
                if ( NEO_DATA_SOURCE_NAME.equals( tx.getDataSourceName() ) )
                {
                    this.txId = tx.getTxId();
                    return;
                }
            }
            throw new IllegalStateException( "No information for data source: " + NEO_DATA_SOURCE_NAME );
        }

        @Override
        protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
        {
            return provider.transaction( ++txId );
        }
    }

    @Override
    public Response<Void> copyStore( RequestContext context, StoreWriter writer )
    {
        throw new UnsupportedOperationException( "slave of fake master should never need seeding" );
    }

    @Override
    public Response<IdAllocation> allocateIds( IdType idType )
    {
        throw readOnly();
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        throw readOnly();
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context, long... relationships )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( RequestContext context, long... relationships )
    {
        throw readOnly();
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context, String resource, TxExtractor
            txGetter )
    {
        throw readOnly();
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        throw readOnly();
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context, String dsName, long startTxId, long endTxId )
    {
        throw readOnly();
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key )
    {
        throw readOnly();
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        throw readOnly();
    }

    @Override
    public Response<Void> pushTransaction( RequestContext context, String resourceName, long tx )
    {
        throw readOnly();
    }

    private static RuntimeException readOnly()
    {
        return new UnsupportedOperationException( "Slave should be read only" );
    }
}
