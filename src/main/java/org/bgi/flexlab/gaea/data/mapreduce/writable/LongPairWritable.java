/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.data.mapreduce.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongPairWritable implements WritableComparable<LongPairWritable> {
    private long first;
    private long second;

    public LongPairWritable() {
    }

    public LongPairWritable(long first, long second) {
        this.set(first, second);
    }

    public void set(long first, long second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 反序列化
     */
    @Override
    public void readFields(DataInput arg0) throws IOException {
        this.first = arg0.readLong();
        this.second = arg0.readLong();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(first);
        arg0.writeLong(second);
    }

    @Override
    public int compareTo(LongPairWritable o) {
        if(first != o.first)
            return first < o.first ? -1 : 1;
        else
            return second < o.second ? -1 : 1;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }
}