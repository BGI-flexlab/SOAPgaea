package org.bgi.flexlab.gaea.data.structure.region;

import java.io.IOException;

import org.bgi.flexlab.gaea.util.FileIterator;

public class RegionHdfsParser extends BasicRegion{
	public void parseBedFileFromHDFS(String bedFilePath, boolean isWithFlank) throws IOException {
		FileIterator it = new FileIterator(bedFilePath);
		while(it.hasNext()) {
			parseBedRegion(it, isWithFlank);
		}
		it.close();
		//System.err.println("region size:" + regionSize);
	}
	
	protected void parseBedRegion(FileIterator it, boolean isWithFlank) {
		String line=it.next().toString().trim();
		String[] splitArray = line.split("\\s+");
		if(line.equals("") || line == null) {
			return;
		}
		
		boolean skipLine = parseBedFileLine(splitArray, this);
		if(skipLine) {
			return;
		} else {
			addChrSize(chrName, calRegionSize(start, end));
		}
		
		if(isWithFlank) {
			while(it.hasNext()) {
				line=it.next().toString().trim();
				splitArray = line.split("\t");
				skipLine = parseBedFileLine(splitArray, flankRegion);
				if(skipLine) {
					continue;
				} else {
					addChrSize(flankRegion.getChrName(), calRegionSize(flankRegion.getStart(), flankRegion.getEnd()));
				}
				
				processWindow();
			}
			//处理最后一个窗口
			processWindow(start, end, chrName);
		}
	}
}
