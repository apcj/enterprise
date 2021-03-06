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

package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupEmbeddedIT.BACKUP_PATH;
import static org.neo4j.backup.BackupEmbeddedIT.PATH;
import static org.neo4j.backup.BackupEmbeddedIT.createSomeData;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.ClusterManager;

public class BackupHaIT
{
    private DbRepresentation representation;
    private ClusterManager clusterManager;

    @Before
    public void startCluster() throws Throwable
    {
        FileUtils.deleteDirectory( new File( PATH ) );
        FileUtils.deleteDirectory( new File( BACKUP_PATH ) );

        clusterManager = new ClusterManager( getClass().getResource( "/threeinstances.xml" ).toURI(),
                new File( PATH ), MapUtil.stringMap( OnlineBackupSettings.online_backup_enabled.name(),
                GraphDatabaseSetting.TRUE ) )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, int serverCount )
            {
                builder.setConfig( OnlineBackupSettings.online_backup_port, (4444 + serverCount) + "" );
            }
        };
        clusterManager.start();

        // Really doesn't matter which instance
        representation = createSomeData( clusterManager.getMaster( "neo4j.ha" ) );
    }

    @After
    public void stopCluster() throws Throwable
    {
        clusterManager.stop();
    }

    @Test
    public void makeSureBackupCanBePerformedFromClusterWithDefaultName() throws Throwable
    {
        testBackupFromCluster( null );
    }

    @Test
    public void makeSureBackupCanBePerformedFromWronglyNamedCluster() throws Throwable
    {
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( true, "ha://localhost:5001", BACKUP_PATH, "non.existent" ) ) );
    }

    private void testBackupFromCluster( String askForCluster ) throws Throwable
    {
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( true, "ha://127.0.0.1:5001", BACKUP_PATH, askForCluster ) ) );
        assertEquals( representation, DbRepresentation.of( BACKUP_PATH ) );
        DbRepresentation newRepresentation = createSomeData( clusterManager.getAnySlave( askForCluster == null ?
                "neo4j.ha" : askForCluster ) );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( false, "ha://127.0.0.1:5002", BACKUP_PATH, askForCluster ) ) );
        assertEquals( newRepresentation, DbRepresentation.of( BACKUP_PATH ) );
    }

    private String[] backupArguments( boolean trueForFull, String from, String to, String clusterName )
    {
        List<String> args = new ArrayList<String>();
        args.add( trueForFull ? "-full" : "-incremental" );
        args.add( "-from" );
        args.add( from );
        args.add( "-to" );
        args.add( to );
        if ( clusterName != null )
        {
            args.add( "-cluster" );
            args.add( clusterName );
        }
        return args.toArray( new String[args.size()] );
    }
}
