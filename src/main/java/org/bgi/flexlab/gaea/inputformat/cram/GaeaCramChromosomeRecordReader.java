package org.bgi.flexlab.gaea.inputformat.cram;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.structure.Container;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bgi.flexlab.gaea.inputformat.cram.index.ChromosomeIndex;

public class GaeaCramChromosomeRecordReader extends GaeaCramRecordReader {
	private int sequenceId = Integer.MIN_VALUE;
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
			fileStart = chromosome.getStart(chrName);
			virtualEnd = chromosome.getEnd(chrName);

			sequenceId = samFileHeader.getSequenceIndex(chrName);
			sin.seek(fileStart);
		}
	}

	@Override
	public boolean nextKeyValue() {
		/* get new container */
		if ((samRecords.size() > 0 && currentIndex >= samRecords.size())
				|| samRecords.size() == 0) {
			Container c = getContainer();
			if(prevSeqId == sequenceId && c.sequenceId != sequenceId){
				return false;
			}
			if (!ContainerRead(c))
				return false;
		}

		final SAMRecord r = samRecords.get(currentIndex);
		r.setHeader(samFileHeader);
		++currentIndex;

		key.set(getKey(r));
		record.set(r);
		return true;
	}
}
