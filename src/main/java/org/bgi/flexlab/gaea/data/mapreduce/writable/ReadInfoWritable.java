package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.bgi.flexlab.gaea.data.structure.pileup2.ReadInfo;
import org.bgi.flexlab.gaea.util.SAMUtils;

public class ReadInfoWritable implements WritableComparable<ReadInfoWritable> {
	private IntWritable pos;
	private ByteWritable MAQ;
	private Text CIGAR;//
	private BytesWritable sequence;
	private Text quality;
	private IntWritable sample;

	public ReadInfoWritable() {
		pos = new IntWritable();
		MAQ = new ByteWritable();
		CIGAR = new Text();
		sequence = new BytesWritable();
		quality = new Text();
		sample = new IntWritable();
	}

	public ReadInfoWritable(IntWritable pos, ByteWritable MAQ, Text CIGAR, BytesWritable sequence, Text quality, IntWritable sample) {
		this.pos = pos;
		this.MAQ = MAQ;
		this.CIGAR = CIGAR;
		this.sequence = sequence;
		this.quality = quality;
		this.sample = sample;
	}

	public ReadInfoWritable(int pos, int MAQ, String CIGAR, String sequence, String quality, int sample) {
		this.pos = new IntWritable(pos);
		if(MAQ>127)
			MAQ = 127;
		this.MAQ = new ByteWritable((byte)(MAQ&0xff));
		this.CIGAR = new Text(CIGAR);
		this.sequence = new BytesWritable();
		this.setsequence(sequence);
		this.quality = new Text(quality);
		this.sample = new IntWritable(sample);
	}
	
	public ReadInfoWritable(ReadInfo readInfo) {
		this.pos = new IntWritable(readInfo.getPosition());
		byte mAQ;
		if(readInfo.getMappingQual() > 127)
			mAQ = 127;
		else
			mAQ = (byte)(readInfo.getMappingQual() & 0xff);
		this.MAQ = new ByteWritable(mAQ);
		this.CIGAR = new Text(readInfo.getCigarString());
		this.sequence = new BytesWritable();
		this.setsequence(readInfo.getReadsSequence());
		this.quality = new Text(readInfo.getQualityString());
		this.sample = new IntWritable(Integer.parseInt(readInfo.getSample()));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pos.readFields(in);
		MAQ.readFields(in);
		CIGAR.readFields(in);
		sequence.readFields(in);
		quality.readFields(in);
		sample.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pos.write(out);
		MAQ.write(out);
		CIGAR.write(out);
		sequence.write(out);
		quality.write(out);
		sample.write(out);
	}

	@Override
	public int hashCode() {
		return pos.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ReadInfoWritable) {
			ReadInfoWritable tp = (ReadInfoWritable) o;
			return pos.equals(tp.getPos());
		}
		return false;
	}

	@Override
	public int compareTo(ReadInfoWritable o) {
		return pos.compareTo(o.getPos());
	}

	public IntWritable getPos() {
		return pos;
	}

	public void setPos(IntWritable pos) {
		this.pos = pos;
	}

	public ByteWritable getMAQ() {
		return MAQ;
	}

	public void setMAQ(ByteWritable mAQ) {
		MAQ = mAQ;
	}

	public Text getCIGAR() {
		return CIGAR;
	}

	public void setCIGAR(Text cIGAR) {
		CIGAR = cIGAR;
	}

	public String getsequence(int length) {
		String seq = null;
		try {
			seq = new String(SAMUtils.compressedBasesToBytes(length, sequence.getBytes(), 0),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return seq;
	}

	public void setsequence(String sequence) {
		byte[] seq = SAMUtils.bytesToCompressedBases(sequence.getBytes());
		int length = seq.length;
		this.sequence.set(seq, 0, length);
	}

	public Text getQuality() {
		return quality;
	}

	public void setQuality(Text quality) {
		this.quality = quality;
	}

	/**
	 * @return the sample
	 */
	public int getSample() {
		return sample.get();
	}

	/**
	 * @param sample the sample to set
	 */
	public void setSample(IntWritable sample) {
		this.sample = sample;
	}
	
	public int length(){
		return quality.toString().length();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		sb.append(getPos().get()+"\t");
		sb.append(getMAQ().get()+"\t");
		sb.append(getCIGAR().toString()+"\t");
		sb.append(this.getsequence(this.length())+"\t");
		sb.append(this.getQuality().toString()+"\t");
		sb.append(this.getSample());
		
		return sb.toString();
	}
}