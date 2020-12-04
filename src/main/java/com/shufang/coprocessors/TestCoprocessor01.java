package com.shufang.coprocessors;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;

public class TestCoprocessor01 implements RegionObserver, Coprocessor {
    public static void main(String[] args) {

    }

    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
        return null;
    }


}
