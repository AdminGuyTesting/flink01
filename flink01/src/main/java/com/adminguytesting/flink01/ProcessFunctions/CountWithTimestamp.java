package com.adminguytesting.flink01.ProcessFunctions;

/**
 * The data type stored in the state
 */
class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}
