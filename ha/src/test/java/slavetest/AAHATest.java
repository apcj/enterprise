/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package slavetest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;
import org.neo4j.test.ha.StandaloneDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public class AAHATest
{
    @Test
    public void createLotsOfData() throws Exception
    {
        int nSlaves = 10;
        initializeDbs( nSlaves, MapUtil.stringMap() );
        int nNodesToCreate = 1000;
        for (int iNode = 0; iNode < nNodesToCreate; iNode++) {
            executeJob( new CreateDataJob(), 0 );
        }
        for (int iSlave = 0; iSlave < nSlaves; iSlave++) {
            int nodeCount = executeJob( new VerifyJob(), 0 );
            assertEquals( nNodesToCreate + 1, nodeCount );
        }
    }

    private static class CreateDataJob implements Job<Void>, Serializable
    {
        @Override
        public Void execute( GraphDatabaseService db ) throws RemoteException
        {
            Transaction transaction = db.beginTx();
            db.createNode();
            transaction.success();
            transaction.finish();
            return null;
        }
    }

    private static class VerifyJob implements Job<Integer>, Serializable
    {
        @Override
        public Integer execute( GraphDatabaseService db ) throws RemoteException
        {
            Iterable<Node> allNodes = GlobalGraphOperations.at( db ).getAllNodes();
            int nodeCount = 0;
            for ( Node node : allNodes )
            {
                nodeCount++;
            }
            return nodeCount;
        }
    }

    private static final File BASE_ZOO_KEEPER_DATA_DIR =
            new File( new File( "target" ), "zookeeper-data" );
    private static final int BASE_HA_SERVER_PORT = 5559;

    private static LocalhostZooKeeperCluster zooKeeperCluster;

    private final Map<Integer, StandaloneDatabase> jvmByMachineId = new HashMap<Integer, StandaloneDatabase>();

    @Before
    public void startZooKeeperCluster() throws Exception
    {
        FileUtils.deleteDirectory( BASE_ZOO_KEEPER_DATA_DIR );
        zooKeeperCluster = new LocalhostZooKeeperCluster( getClass(), /*ports:*/2181, 2182, 2183 );
    }

    protected void initializeDbs( int numSlaves, Map<String, String> config ) throws Exception
    {
        startUpMaster( config );
        for ( int i = 0; i < numSlaves; i++ )
        {
            addDb( config, true );
        }
        for ( StandaloneDatabase db : jvmByMachineId.values() )
        {
            db.awaitStarted();
        }
    }

    protected StandaloneDatabase spawnJvm( File path, int machineId, String... extraArgs ) throws Exception
    {
        StandaloneDatabase db = StandaloneDatabase.withDefaultBroker( testName.getMethodName(),
                path.getAbsoluteFile(), machineId, zooKeeperCluster,
                buildHaServerConfigValue( machineId ), extraArgs );
        jvmByMachineId.put( machineId + 1, db );
        return db;
    }

    private static String buildHaServerConfigValue( int machineId )
    {
        return "localhost:" + (BASE_HA_SERVER_PORT + machineId);
    }

    @After
    public void shutdownZooKeeperCluster()
    {
        zooKeeperCluster.shutdown();
    }

    private final List<StandaloneDatabase> jvms = new ArrayList<StandaloneDatabase>();

    protected int addDb( Map<String, String> config, boolean awaitStarted ) throws Exception
    {
        int machineId = jvms.size();
        jvms.add( startDb( machineId, config, awaitStarted ) );
        return machineId;
    }

    protected StandaloneDatabase startDb( int machineId, Map<String, String> config, boolean awaitStarted ) throws Exception
    {
        File slavePath = dbPath( machineId );
        StandaloneDatabase slaveJvm = spawnJvm( slavePath, machineId, buildExtraArgs( config ) );
        if ( awaitStarted )
        {
            slaveJvm.awaitStarted();
        }
        return slaveJvm;
    }

    protected static String[] buildExtraArgs( Map<String, String> config )
    {
        List<String> list = new ArrayList<String>();
        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            list.add( "-" + entry.getKey() );
            list.add( entry.getValue() );
        }
        return list.toArray( new String[list.size()] );
    }

    protected void shutdownDb( int machineId )
    {
        jvms.get( machineId ).kill();
    }

    @After
    public void shutdownDbs() throws Exception
    {
        for ( StandaloneDatabase slave : jvms )
        {
            slave.shutdown();
        }
    }

    protected void pullUpdates( int... slaves ) throws Exception
    {
        if ( slaves.length == 0 )
        {
            for ( int i = 1; i < jvms.size(); i++ )
            {
                jvms.get( i ).pullUpdates();
            }
        }
        else
        {
            for ( int slave : slaves )
            {
                jvms.get( slave+1 ).pullUpdates();
            }
        }
    }

    protected <T> T executeJob( Job<T> job, int onSlave ) throws Exception
    {
        return jvms.get( onSlave+1 ).executeJob( job );
    }


    protected void startUpMaster( Map<String, String> config ) throws Exception
    {
        Map<String, String> newConfig = new HashMap<String, String>( config );
        newConfig.put( "master", "true" );
        StandaloneDatabase com = spawnJvm( dbPath( 0 ), 0, buildExtraArgs( newConfig ) );
        if ( jvms.isEmpty() )
        {
            jvms.add( com );
        }
        else
        {
            jvms.set( 0, com );
        }
        com.awaitStarted();
    }

    // from AbstractHaTest

    static final File PARENT_PATH = new File( "target"+File.separator+"havar" );
    static final File DBS_PATH = new File( PARENT_PATH, "dbs" );

    private int maxNum;

    public @Rule
    TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return AAHATest.this.getClass().getName() + "." + super.getMethodName();
        }
    };


    protected String getStorePrefix()
    {
        return "0-";
    }


    protected File dbPath( int num )
    {
        maxNum = Math.max( maxNum, num );
        return new File( DBS_PATH, getStorePrefix() + num );
    }

    @After
    public void clearDb() throws IOException
    {
        for ( int i = 0; i <= maxNum; i++ )
        {
            org.neo4j.kernel.impl.util.FileUtils.deleteRecursively( dbPath( i ) );
        }
    }


}
