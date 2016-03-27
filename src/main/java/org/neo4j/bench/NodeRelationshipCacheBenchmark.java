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
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;

@OutputTimeUnit( TimeUnit.SECONDS )
@State( Scope.Benchmark )
public class NodeRelationshipCacheBenchmark
{
    private NodeRelationshipCache cache;
    @Param( value = {"50"} )
    int denseNodeThreshold;

    @Benchmark
    public void getAndPut( Tlr random )
    {
        cache.getAndPutRelationship( random.rng.nextLong( 100_000_000 ), random.rng.nextInt( 10 ),
                Direction.OUTGOING, random.rng.nextLong( 1_000_000_000 ), true );
    }

    @Setup
    public void setUp()
    {
        cache = new NodeRelationshipCache( HEAP, denseNodeThreshold );
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
