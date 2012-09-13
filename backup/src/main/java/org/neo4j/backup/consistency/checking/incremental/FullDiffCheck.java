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
package org.neo4j.backup.consistency.checking.incremental;

import org.neo4j.backup.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.backup.consistency.checking.full.FullCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullDiffCheck extends DiffCheck
{
    public FullDiffCheck( StringLogger logger )
    {
        super( logger );
    }

    @Override
    public void execute( DiffStore diffs, ConsistencyReport.Reporter reporter )
            throws ConsistencyCheckIncompleteException
    {
        new FullCheck( false, false, ProgressMonitorFactory.NONE ).execute( diffs, reporter );
    }
}
