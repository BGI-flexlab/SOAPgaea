package org.bgi.flexlab.gaea.data.mapreduce.input.cram;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GaeaCramChromosomeRecordReader extends GaeaCramRecordReader {
	private int sequenceId = Integer.MIN_VALUE;
	private int prevSeqId = -1;
	public final static String CHROMOSOME = "chromosome.name";

	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException {
		super.initialize(inputSplit, context);

		FileSplit split = (FileSplit) inputSplit;
		final Path file = split.getPath();

		String chrName = context.getConfiguration().get(CHROMOSOME);
		String indexPath = context.getConfiguration().get("cram.index.path");

		if (chrName != null) {
			ChromosomeIndex chromosome = null;
			if (indexPath == null)
				chromosome = new ChromosomeIndex(file.toString());
			else
				chromosome = new ChromosomeIndex(file.toString(), indexPath
						+ "/" + file.getName() + ".crai");
			chromosome.setHeader(samFileHeader);
			start = chromosome.getStart(chrName);
			length = chromosome.getEnd(chrName) - start;
			
			sequenceId = samFileHeader.getSequenceIndex(chrName);
			seekableStream.seek(start);
		}
	}

	@Override
	public boolean nextKeyValue() {
		/* get new container */
		boolean res = super.nextKeyValue();
		int currSequenceId = record.get().getReferenceIndex();
		
		if(prevSeqId == sequenceId && currSequenceId != sequenceId){
			return false;
		}

		return res;
	}
}
