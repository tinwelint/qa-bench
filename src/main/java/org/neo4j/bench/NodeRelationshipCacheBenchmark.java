/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;

import static java.lang.Long.max;
import static java.lang.Math.abs;

import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;

@OutputTimeUnit( TimeUnit.SECONDS )
@State( Scope.Benchmark )
public class NodeRelationshipCacheBenchmark
{
    private static final Direction[] DIRECTIONS = Direction.values();

    private NodeRelationshipCache cache;
    @Param( {"50"} )
    int denseNodeThreshold;
    @Param( {"100000000"} )
    long nodes;
    @Param( {"10"} )
    int relationshipTypes;
    private long relationships;

    @Benchmark
    public void getAndPut( Tlr random )
    {
        long rnd = max( 1, abs( random.rng.nextLong() ) );
        long nodeId = rnd % nodes;
        int relationshipType = (int)(rnd % relationshipTypes);
        Direction direction = DIRECTIONS[(int)(rnd % DIRECTIONS.length)];
        long relationshipId = rnd % relationships;
        cache.getAndPutRelationship( nodeId, relationshipType, direction, relationshipId, true );
    }

    @Setup
    public void setUp()
    {
        cache = new NodeRelationshipCache( HEAP, denseNodeThreshold );
        relationships = nodes * 100;
    }

    @TearDown
    public void tearDown()
    {
        cache.close();
    }

    @State( Scope.Thread )
    public static class Tlr
    {
        ThreadLocalRandom rng;

        @Setup
        public void setup()
        {
            rng = ThreadLocalRandom.current();
        }
    }
}
