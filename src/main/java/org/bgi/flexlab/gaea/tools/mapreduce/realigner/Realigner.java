package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;

public class Realigner extends ToolsRunner{
	
	public Realigner() {
		this.toolsDescription = "Gaea realigner\n";
	}
	
	public int runRealigner(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		RealignerOptions option = new RealignerOptions();
		option.parse(args);
		
		BioJob job = BioJob.getInstance();
		job.setJobName("GaeaRealigner");
		
		Configuration conf = job.getConfiguration();
		option.setHadoopConf(args, conf);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		return 0;
	}
}
