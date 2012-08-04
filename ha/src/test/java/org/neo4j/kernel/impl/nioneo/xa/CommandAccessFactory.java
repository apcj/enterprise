package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class CommandAccessFactory
{
    public static Command command( RelationshipRecord relationship )
    {
        return new Command.RelationshipCommand( null, relationship );
    }

    public static Command command( NodeRecord node )
    {
        return new Command.NodeCommand( null, node );
    }

    public static Command command( PropertyRecord property )
    {
        return new Command.PropertyCommand( null, property );
    }

    public static Command command( PropertyIndexRecord index )
    {
        return new Command.PropertyIndexCommand( null, index );
    }

    public static Command command( RelationshipTypeRecord type )
    {
        return new Command.RelationshipTypeCommand( null, type );
    }
}
