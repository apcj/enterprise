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
package org.neo4j.kernel.ha;

import static org.neo4j.com.TxChecksumVerifier.ALWAYS_MATCH;

import java.io.IOException;

import org.jboss.netty.channel.Channel;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.kernel.ha.SlaveClient.SlaveRequestType;
import org.neo4j.kernel.impl.util.StringLogger;

public class SlaveServer extends Server<Slave, Void>
{
    public static final byte APPLICATION_PROTOCOL_VERSION = 1;

    public SlaveServer( Slave requestTarget, Configuration config, StringLogger logger )
            throws IOException
    {
        super( requestTarget, config, logger, DEFAULT_FRAME_LENGTH, APPLICATION_PROTOCOL_VERSION, ALWAYS_MATCH );
    }

    @Override
    protected RequestType<Slave> getRequestContext( byte id )
    {
        return SlaveRequestType.values()[id];
    }

    @Override
    protected void finishOffChannel( Channel channel, RequestContext context )
    {
    }
}
