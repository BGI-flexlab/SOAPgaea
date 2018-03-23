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
package org.bgi.flexlab.gaea.tools.mapreduce.bamsort;

import htsjdk.samtools.ReservedTagConstants;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamFileHeaderMerger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.seqdoop.hadoop_bam.BAMRecordReader;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.MurmurHash3;

public class SortRecordReader extends
		RecordReader<LongWritable, SAMRecordWritable> {
	private final RecordReader<LongWritable, SAMRecordWritable> baseRR;

	private SamFileHeaderMerger headerMerger;
	private Configuration conf;
	private HashMap<String,Integer> sampleID = new HashMap<String,Integer>();
	private boolean multiSample = false;

	public SortRecordReader(RecordReader<LongWritable, SAMRecordWritable> rr) {
		baseRR = rr;
	}

	@Override
	public void initialize(InputSplit spl, TaskAttemptContext ctx)
			throws InterruptedException, IOException {
		baseRR.initialize(spl, ctx);
		
		conf = ctx.getConfiguration();
		headerMerger = BamSortUtils.getHeaderMerger(ctx.getConfiguration());
		
		List<SAMReadGroupRecord> list = headerMerger.getMergedHeader().getReadGroups();
		
		for(int i=0;i<list.size();i++)
			sampleID.put(list.get(i).getSample(), i);
		
		multiSample = conf.getBoolean("multi.samples", false);
	}

	@Override
	public void close() throws IOException {
		if(baseRR != null)
			baseRR.close();
		sampleID.clear();
	}

	@Override
	public float getProgress() throws InterruptedException, IOException {
		return baseRR.getProgress();
	}

	@Override
	public LongWritable getCurrentKey() throws InterruptedException,
			IOException {
		return baseRR.getCurrentKey();
	}

	@Override
	public SAMRecordWritable getCurrentValue() throws InterruptedException,
			IOException {
		return baseRR.getCurrentValue();
	}
	
	public long setKey(SAMRecord r) throws InterruptedException, IOException{
		long newKey = 0;
		int ridx = r.getReferenceIndex();
		int start = r.getAlignmentStart();
		
		if(multiSample){
			String sample = r.getReadGroup().getSample();
			if(!sampleID.containsKey(sample))
				throw new RuntimeException("cantains not sample "+sample+"\t"+sampleID.size());
			int idIndex = sampleID.get(sample);
			
			if((ridx < 0 || start < 0)){
				int hash = 0;
				byte[] var;
				if ((var = r.getVariableBinaryRepresentation()) != null) {
					// Undecoded BAM record: just hash its raw data.
					hash = (int)MurmurHash3.murmurhash3(var, hash);
				} else {
					// Decoded BAM record or any SAM record: hash a few representative
					// fields together.
					hash = (int)MurmurHash3.murmurhash3(r.getReadName(), hash);
					hash = (int)MurmurHash3.murmurhash3(r.getReadBases(), hash);
					hash = (int)MurmurHash3.murmurhash3(r.getBaseQualities(), hash);
					hash = (int)MurmurHash3.murmurhash3(r.getCigarString(), hash);
				}
				hash = Math.abs(hash);
				newKey = ((long)idIndex << 48) | ((long)65535 << 32) | (long)hash;
			}else{
				newKey = ((long)idIndex << 48) | (((long)ridx) << 32) | ((long)start-1);
			}
			getCurrentKey().set(newKey);
		}else{
			if(ridx != -1 && r.getAlignmentStart() != -1)
				getCurrentKey().set(BAMRecordReader.getKey(ridx,start));
		}
		
		return  newKey;
	}

	@Override
	public boolean nextKeyValue() throws InterruptedException, IOException {
		if (!baseRR.nextKeyValue()){
			return false;
		}

		final SAMRecord r = getCurrentValue().get();
		final int ridx = r.getReferenceIndex();

		final SAMFileHeader h = r.getHeader();

		// Correct the reference indices, and thus the key, if necessary.
		if (headerMerger.hasMergedSequenceDictionary()) {
			int ri = headerMerger.getMergedSequenceIndex(h,
					r.getReferenceIndex());

			r.setReferenceIndex(ri);
			if (r.getReadPairedFlag())
				r.setMateReferenceIndex(headerMerger.getMergedSequenceIndex(h,
						r.getMateReferenceIndex()));
		}

		// Correct the program group if necessary.
		if (headerMerger.hasProgramGroupCollisions()) {
			final String pg = (String) r
					.getAttribute(ReservedTagConstants.PROGRAM_GROUP_ID);
			if (pg != null)
				r.setAttribute(ReservedTagConstants.PROGRAM_GROUP_ID,
						headerMerger.getProgramGroupId(h, pg));
		}

		// Correct the read group if necessary.
		if (headerMerger.hasReadGroupCollisions()) {
			final String rg = (String) r
					.getAttribute(ReservedTagConstants.READ_GROUP_ID);
			if (rg != null)
				r.setAttribute(ReservedTagConstants.READ_GROUP_ID,
						headerMerger.getProgramGroupId(h, rg));
		}

		setKey(r);
		
		getCurrentValue().set(r);
		return true;
	}
}
