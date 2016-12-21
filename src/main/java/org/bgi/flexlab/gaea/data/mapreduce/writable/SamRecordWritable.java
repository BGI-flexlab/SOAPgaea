package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory;
import org.seqdoop.hadoop_bam.util.DataInputWrapper;
import org.seqdoop.hadoop_bam.util.DataOutputWrapper;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMRecord;

public class SamRecordWritable implements Writable{
	private static final BamRecordCodec lazyCodec =
			new BamRecordCodec(null, new LazyBAMRecordFactory());

		private SAMRecord record;

		public SAMRecord get()            { return record; }
		public void      set(SAMRecord r) { record = r; }

		@Override 
		public void write(DataOutput out) throws IOException {
			// In theory, it shouldn't matter whether we give a header to
			// BAMRecordCodec or not, since the representation of an alignment in BAM
			// doesn't depend on the header data at all. Only its interpretation
			// does, and a simple read/write codec shouldn't really have anything to
			// say about that. (But in practice, it already does matter for decode(),
			// which is why LazyBAMRecordFactory exists.)
			final BAMRecordCodec codec = new BAMRecordCodec(record.getHeader());
			codec.setOutputStream(new DataOutputWrapper(out));
			codec.encode(record);
		}
		
		@Override 
		public void readFields(DataInput in) throws IOException {
			lazyCodec.setInputStream(new DataInputWrapper(in));
			record = lazyCodec.decode();
		}

		@Override
		public String toString() {
			return record.getSAMString().trim(); // remove trailing newline
		}
}
