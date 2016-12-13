package org.bgi.flexlab.gaea.data.structure.dbsnp;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bgi.flexlab.gaea.data.structure.memoryshare.WholeGenomeShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader.PositionalVariantContext;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;

public class DbsnpShare extends WholeGenomeShare {
	private static final String CACHE_NAME = "dbsnpList";

	private Map<String, ChromosomeDbsnpShare> dbsnpInfo = new ConcurrentHashMap<String, ChromosomeDbsnpShare>();

	public static boolean distributeCache(String chrList, Job job) {
		try {
			return distributeCache(chrList, job, CACHE_NAME);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.toString());
		}
	}

	public void loadChromosomeList() {
		loadChromosomeList(CACHE_NAME);
	}

	public void loadChromosomeList(String chrList) {
		try {
			loadChromosomeList(new Path(chrList));
		} catch (IllegalArgumentException | IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	@Override
	public boolean addChromosome(String chrName) {
		ChromosomeDbsnpShare newChr = new ChromosomeDbsnpShare();
		dbsnpInfo.put(chrName, newChr);
		if (dbsnpInfo.get(chrName) != null) {
			return true;
		} else {
			return false;
		}
	}

	public ChromosomeDbsnpShare getChromosomeDbsnp(String chrName) {
		chrName = ChromosomeUtils.formatChrName(chrName);
		if (!dbsnpInfo.containsKey(chrName))
			throw new RuntimeException("dbsnp not contains reference " + chrName);
		return dbsnpInfo.get(chrName);
	}

	public long getStartPosition(String chrName, int winNum, int winSize) {
		chrName = ChromosomeUtils.formatChrName(chrName);
		if (!dbsnpInfo.containsKey(chrName))
			throw new RuntimeException("dbsnp not contains reference " + chrName);

		return dbsnpInfo.get(chrName).getStartPosition(winNum, winSize);
	}

	public long getStartPosition(String chrName, int winNum) {
		return getStartPosition(chrName, winNum, 0);
	}

	public Map<String, ChromosomeDbsnpShare> getDbsnpMap() {
		return this.dbsnpInfo;
	}

	public void setChromosome(String path, String chrName, int length) {
		if (dbsnpInfo.containsKey(chrName)) {
			// map chr and get length
			dbsnpInfo.get(chrName).loadChromosome(path);
			dbsnpInfo.get(chrName).setLength(length);
			dbsnpInfo.get(chrName).setChromosomeName(chrName);
		}
	}

	public static void main(String[] args) {
		DbsnpShare share = new DbsnpShare();

		share.loadChromosomeList(args[0]);

		ChromosomeDbsnpShare dbshare = share.getChromosomeDbsnp(ChromosomeUtils.formatChrName(args[1]));
		
		int winSize = 100;
		if(args.length > 3)
			winSize = Integer.parseInt(args[3]);
		
		int length = dbshare.getLength();

		VCFLocalLoader reader;
		try {
			reader = new VCFLocalLoader(args[2]);
		} catch (IOException e1) {
			throw new RuntimeException(e1.toString());
		}
		
		for(int i = 0 ; i < (length / winSize) ; i++){
			long position = dbshare.getStartPosition(i,winSize);
			try {
				if(position < 0)
					continue;
				
				reader.seek(position);
				
				while(reader.hasNext()){
					PositionalVariantContext context = reader.next();
					System.out.println(i+"\t"+position+"\t"+context.getVariantContext().getStart());
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		reader.close();
	}
}
