package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasicCodec;
import org.seqdoop.hadoop_bam.util.DataInputWrapper;
import org.seqdoop.hadoop_bam.util.DataOutputWrapper;

public class AlignmentBasicWritable implements WritableComparable<AlignmentBasicWritable> {
	private static final AlignmentsBasicCodec codec = new AlignmentsBasicCodec();

	private AlignmentsBasic alignment;

	public AlignmentsBasic getAlignment() {
		return alignment;
	}

	public void setAlignment(AlignmentsBasic alignment) {
		this.alignment = alignment;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		codec.setInputStream(new DataInputWrapper(dataInput));
		alignment = codec.decode();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		//final AlignmentsBasicCodec codec = new AlignmentsBasicCodec();
		codec.setOutputStream(new DataOutputWrapper(dataOutput));
		codec.encode(alignment);
	}

	@Override
	public int compareTo(AlignmentBasicWritable o) {
		AlignmentsBasic alignment2 = o.getAlignment();
		if(alignment.getChrNameIndex() != alignment2.getChrNameIndex()) {
			if(alignment.getChrNameIndex() > alignment2.getChrNameIndex()) {
				return 1;
			} else {
				return -1;
			}
		} else {
			if(alignment.getPosition() != alignment2.getPosition()) {
				if(alignment.getPosition() > alignment2.getPosition()) {
					return 1;
				} else {
					return -1;
				}
			}
		}
		return 0;
	}
	
}