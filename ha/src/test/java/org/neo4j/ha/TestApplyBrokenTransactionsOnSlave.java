package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class TestApplyBrokenTransactionsOnSlave
{
    @Rule
    public final FakeMasterFixture fixture = new FakeMasterFixture();

    @Test
    public void shouldApplyValidTransactionsWithoutError() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase db = fixture.startDatabase( FakeMasterFixture.EMPTY_INITIAL_DATABASE );

        fixture.inject( new FakeMasterFixture.MasterTransaction()
        {
            @Override
            public void transactionData( FakeMasterFixture.TransactionCommandFactory tx )
            {
                tx.relationshipType( 0, "self" );
                tx.update( new NodeRecord( 0, 0, -1 ) );
                tx.create( new RelationshipRecord( 0, 0, 0, 0 ) );
            }
        } );

        // when
        db.pullUpdates();

        // then
        Node root = db.getReferenceNode();
        Relationship selfReference = root.getSingleRelationship( withName( "self" ), Direction.OUTGOING );
        assertEquals( root, selfReference.getEndNode() );
    }

    @Test
    public void shouldTriggerRecoveryAfterApplyingATransactionFailsDueToRuntimeException() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase db = fixture.startDatabase( FakeMasterFixture.EMPTY_INITIAL_DATABASE );

        fixture.inject( new FakeMasterFixture.MasterTransaction()
        {
            @Override
            protected void transactionData( FakeMasterFixture.TransactionCommandFactory tx )
            {
                tx.create( new NodeRecord( 1, 0, -1 ) );
            }
        } );
        db.pullUpdates();
        assertNotNull( "node 1 should be created", db.getNodeById( 1 ) );
        fixture.inject( new FakeMasterFixture.MasterTransaction()
        {
            boolean broken = true;

            @Override
            protected void transactionData( FakeMasterFixture.TransactionCommandFactory tx )
            {
                if ( broken )
                { // only send bad data the first time, to allow the db to recover
                    tx.delete( new NodeRecord( -100, 0, 0 ) );
                    broken = false;
                }
                tx.delete( new NodeRecord( 1, 0, -1 ) );
            }
        } );

        // when
        try
        {
            db.pullUpdates();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        // then - give the database tons of chances to recover before we call this "failed"
        Node node1 = null;
        try
        {
            node1 = db.getNodeById( 1 );
        }
        catch ( NotFoundException expected )
        {
            // do nothing
        }
        if ( node1 != null )
        {
            // try restart to force recovery
            fixture.internalRestart( false );
        }
        db.pullUpdates();
        node1 = null;
        try
        {
            node1 = db.getNodeById( 1 );
        }
        catch ( NotFoundException expected )
        {
            // do nothing
        }
        assertNull( "node 1 should have been removed", node1 );
    }
}
