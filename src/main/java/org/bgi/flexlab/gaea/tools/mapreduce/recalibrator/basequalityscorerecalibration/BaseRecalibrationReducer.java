package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.basequalityscorerecalibration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.context.AlignmentContext;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.manager.PileupState;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.util.Window;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;

public class BaseRecalibrationReducer extends Reducer<WindowsBasedWritable, SAMRecordWritable, NullWritable, Text> {

	BaseRecalibrationOptions options = null;
	/**
	 * win size
	 */
	private int winSize;
	/**
	 * win number
	 */
	private int winNum=-1;
	/**
	 * reference name
	 */
	private String referenceName="*";
	/**
	 * windows start pos
	 */
	private int start=-1;
	/**
	 * window stop pos
	 */
	private int stop=-1;		
	/**
	 * chromosome information
	 */
	private static GenomeShare genome;

	private SAMFileHeader mFileHeader=null;
	
	private BaseRecalibrator br = null;
	
	private GenomeLocationParser genomeLocParser;
	
	public static Context ctx = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration job = context.getConfiguration();
		options = new BaseRecalibrationOptions();;
		options.parse(job.getStrings("args"));
		winSize = options.getWinSize();
		
		genome = new GenomeShare();	
		if(job.get("cacheref")==null)
			genome.loadChromosomeList(options.getReferenceSequencePath());
		else
			genome.loadChromosomeList();
		mFileHeader = SamFileHeader.getHeader(job);
		
		genomeLocParser = new GenomeLocationParser(mFileHeader.getSequenceDictionary());
		
		br = new BaseRecalibrator(mFileHeader, options);/**/
		br.initialize();
	}
	
	@Override
	public void reduce(WindowsBasedWritable key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
		ctx = context;
		getKeyInfo(key);
		setWin();
		initKnowSite();
		ArrayList<GaeaSamRecord> records = retrieveRecords(values.iterator());
		ChromosomeInformationShare chrInfo = genome.getChromosomeInfo(referenceName);
		if(chrInfo == null){
			context.getCounter("ERROR", referenceName+"is no in reference").increment(1);
			return;
		}
		PileupState manager=new PileupState(records, genomeLocParser);
		AlignmentContext locus=null;
		while (manager.hasNext()) {
			 locus=manager.next();
			 if(locus.getPosition()<start)
				 continue;
			 if(locus.getPosition()>stop)
				 break;
			br.map((byte)chrInfo.getBase((int)locus.getPosition()-1), locus);
		}
	}
	
	protected void cleanup(Context context) throws IOException,InterruptedException {
		br.print(context);
    }
	
	private void setWin() {
		start=winNum*winSize;
		stop=(winNum+1)*winSize-1<mFileHeader.getSequence(referenceName).getSequenceLength()?(winNum+1)*winSize-1:mFileHeader.getSequence(referenceName).getSequenceLength();
	}
	
	private void getKeyInfo(WindowsBasedWritable key) {
		referenceName = key.getChromosomeName();
		winNum = key.getWindowsNumber(); 
	}
	
	private void initKnowSite() {
		if(options.getKnowSite() != null) {
			Window win = new Window(referenceName,start,stop);
			br.initKnowSite(win);
		}
	}
	
	private ArrayList<GaeaSamRecord> retrieveRecords(Iterator<SAMRecordWritable> values) {
		ArrayList<GaeaSamRecord> records = new ArrayList<>();
		while(values.hasNext()) {
			GaeaSamRecord aRecord = new GaeaSamRecord(mFileHeader, values.next().get());
			records.add(aRecord);
		}
		return records;
	}
}


