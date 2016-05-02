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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.LimitedRecordGenerators;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordGenerators;
import org.neo4j.kernel.impl.store.format.RecordGenerators.Generator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.test.Randoms;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardOpenOption.CREATE;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@OutputTimeUnit( TimeUnit.MILLISECONDS )
@State( Scope.Benchmark )
public class RecordFormatBenchmark
{
    private static final int PAGE_SIZE = (int) kibiBytes( 8 );

    private AbstractBaseRecord[] records;
    @SuppressWarnings( "rawtypes" )
    private RecordFormat format;
    private PageCache pageCache;
    private EphemeralFileSystemAbstraction fs;
    private int recordSize;
    private BatchingIdSequence idSequence;
    private PagedFile storeFile;

    // Configure "high level" limit to record ids, i.e. "community", "enterprise" ...
    @Param( value = {"community"} )
    String limit;
    @Param( value = {"0"} ) // ... or number of bits, if not further specified by specific param below
    int bits;
    @Param( value = {"0"} ) // == 'bits' if not specified
    int entityBits;
    @Param( value = {"0"} ) // == 'bits' if not specified
    int propertyBits;
    @Param( value = {"org.neo4j.kernel.impl.store.format.standard.StandardV3_0"} ) // RecordFormats
    String formats;
    @Param( value = {"node"} ) // RecordFormat, i.e. method name to call on RecordFormats instance
    String type;
    @Param( value = {"true"} )
    boolean singleUnit; // whether or not to only generate records that occupy a single unit

    @SuppressWarnings( "rawtypes" )
    @Setup
    public void before() throws Throwable
    {
        // Determine the limits
        Limits limits = Limits.valueOf( limit );
        if ( propertyBits == 0 )
        {
            propertyBits = bits;
            if ( propertyBits == 0 )
            {
                propertyBits = limits.propertyBits;
            }
        }
        if ( entityBits == 0 )
        {
            entityBits = bits;
            if ( entityBits == 0 )
            {
                entityBits = limits.entityBits;
            }
        }

        // Instantiate the record format
        Class<? extends RecordFormats> formatsClass = Class.forName( formats ).asSubclass( RecordFormats.class );
        RecordFormats recordFormats = formatsClass.newInstance();
        format = (RecordFormat) formatsClass.getMethod( type ).invoke( recordFormats );

        // And generate the records
        Randoms random = new Randoms();
        RecordGenerators generators = new LimitedRecordGenerators( random, entityBits, propertyBits, 40, 16,
                Record.NULL_REFERENCE.intValue() );
        Generator generator = (Generator) generators.getClass().getMethod( type ).invoke( generators );
        records = new AbstractBaseRecord[100];
        recordSize = format.getRecordSize( new IntStoreHeader( 100 ) );
        for ( int i = 0; i < records.length; i++ )
        {
            records[i] = generateRecord( generator, i );
        }
        idSequence = new BatchingIdSequence( 200 );

        // And the PageCache to act as target
        fs = new EphemeralFileSystemAbstraction();
        Map<String,String> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        pageCache = StandalonePageCacheFactory.createPageCache( fs, PageCacheTracer.NULL, new Config( settings ) );
        storeFile = pageCache.map( new File( "store" ), PAGE_SIZE, CREATE );

        // And write the record so that a read benchmark works fine by just reading right after we've set this up
        writeRecords();
    }

    @SuppressWarnings( {"rawtypes", "unchecked"} )
    private AbstractBaseRecord generateRecord( Generator generator, long id )
    {
        long safeId = 1_000;
        BatchingIdSequence tempIdSequence = new BatchingIdSequence( safeId );
        while ( true )
        {
            tempIdSequence.reset();
            AbstractBaseRecord record = generator.get( recordSize, format, id );
            format.prepare( record, recordSize, tempIdSequence );
            if ( (tempIdSequence.peek() == safeId) == singleUnit && record.inUse() )
            {
                return record;
            }
        }
    }

    @TearDown
    public void after() throws Throwable
    {
        storeFile.close();
        pageCache.close();
        fs.shutdown();
    }

    @SuppressWarnings( "unchecked" )
    private void writeRecords() throws IOException
    {
        try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            assertedNext( cursor );
            for ( AbstractBaseRecord record : records )
            {
                int offset = toIntExact( record.getId() * recordSize );
                cursor.setOffset( offset );
                format.prepare( record, recordSize, idSequence );
                format.write( record, cursor, recordSize, storeFile );
            }
        }
    }

    private void assertedNext( PageCursor cursor ) throws IOException
    {
        if ( !cursor.next() )
        {
            throw new IllegalStateException( "Couldn't go to next page" );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Benchmark
    public void read() throws IOException
    {
        try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            assertedNext( cursor );
            for ( AbstractBaseRecord record : records )
            {
                // Perhaps not necessary to check retry and out of bounds in this benchmark?
                int offset = toIntExact( record.getId() * recordSize );
//                do
//                {
                    cursor.setOffset( offset );
                    format.read( record, cursor, NORMAL, recordSize, storeFile );
//                }
//                while ( cursor.shouldRetry() );
//                if ( cursor.checkAndClearBoundsFlag() )
//                {
//                    throw new IllegalStateException( "Out-of-bounds when reading record " + record );
//                }
            }
        }
    }

    @Benchmark
    public void write() throws IOException
    {
        idSequence.reset();
        writeRecords();
    }

    enum Limits
    {
        community( 35, 36 ),
        high( 50, 50 );

        private final int entityBits;
        private final int propertyBits;

        private Limits( int entityBits, int propertyBits )
        {
            this.entityBits = entityBits;
            this.propertyBits = propertyBits;
        }
    }
}
