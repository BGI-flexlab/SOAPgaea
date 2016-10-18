package org.bgi.flexlab.gaea.tools.fastqqualitycontrol.report;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqMultipleSample;

public class FastqQualityControlReporterIO {
	private FastqMultipleSample sample = null;
	private boolean isMulti;

	public FastqQualityControlReporterIO(FastqMultipleSample sample,
			boolean isMulti) {
		this.sample = sample;
		this.isMulti = isMulti;
	}

	public void read(String file, FastqQualityControlReport report)
			throws IOException {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		String line;
		int sampleID = 0, i;
		while ((line = br.readLine()) != null) {
			sampleID = report.addCount(line);

			for (i = 0; i < FastqQualityControlReport.BASE_STATIC_COUNT; i++) {
				line = br.readLine();
				report.addBaseByPosition(sampleID, i, line);
			}
		}

		br.close();
	}

	public void readFromHdfs(Path path, Configuration conf,
			FastqQualityControlReport report) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream FSinput = fs.open(path);

		LineReader lineReader = new LineReader(FSinput, conf);
		System.out.println("path:"+path.toString());
		Text line = new Text();
		int sampleID = 0, i,cnt;
		while ((lineReader.readLine(line)) != 0) {
			System.out.println("sample:"+line.toString());
			sampleID = report.addCount(line.toString());
			if(report.isPartitionNull())
				continue;

			for (i = 0; i < FastqQualityControlReport.BASE_STATIC_COUNT; i++) {
				cnt = lineReader.readLine(line);
				System.out.println("position:"+line.toString());
				if(cnt == 0)
					continue;
				report.addBaseByPosition(sampleID, i, line.toString());
			}
		}

		lineReader.close();
	}

	private void write(String fileName, Configuration conf, String report)
			throws IOException {
		Path reportFilePath = new Path(fileName);
		FileSystem fs = reportFilePath.getFileSystem(conf);
		FSDataOutputStream writer = fs.create(reportFilePath);
		writer.write(report.getBytes());
		writer.close();
	}
	
	static class StaticPathFilter implements PathFilter {
		@Override
		public boolean accept(Path path) {
			if (path.getName().startsWith("filterStatistic"))
				return true;
			return false;
		}
	}

	public void mergeReport(Path input, Configuration conf, Path outputDir)
			throws IOException {
		FileSystem fs = input.getFileSystem(conf);
		FileStatus filelist[] = fs.listStatus(input,new StaticPathFilter());

		int ssize = sample == null ? 1 : sample.getSampleNumber();
		
		FastqQualityControlReport report = new FastqQualityControlReport(
				ssize, isMulti);
		for (int i = 0; i < filelist.length; i++) {
			if (!filelist[i].isDirectory()) {
				readFromHdfs(filelist[i].getPath(), conf, report);
				//fs.delete(filelist[i].getPath(), false);
			}
		}

		fs.close();

		for (int i = 0; i < ssize; i++) {
			String reportFileName;
			String graphFileName;
			if (sample != null && isMulti) {
				String fileName = sample.getFileNameForId(i);
				reportFileName = outputDir + "/" + fileName
						+ ".filter.report.txt";
				graphFileName = outputDir + "/" + fileName + ".graph.data.txt";
			} else {
				reportFileName = outputDir + "/filter.report.txt";
				graphFileName = outputDir + "/graph.data.txt";
			}
			write(reportFileName, conf, report.getReportContext(i));
			write(graphFileName, conf, report.getGraphContext(i));
		}
	}
}
