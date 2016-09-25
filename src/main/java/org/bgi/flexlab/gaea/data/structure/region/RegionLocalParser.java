package org.bgi.flexlab.gaea.data.structure.region;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class RegionLocalParser extends BasicRegion{
	@Deprecated
	public void parseBedFile(String bedFilePath, boolean isWithFlank) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(bedFilePath));
		String line = "";
		String[] splitArray;

		while((line = br.readLine()) != null) {
			splitArray = line.split("\t");
			boolean skipLine = parseBedFileLine(splitArray, this);
			if(skipLine) {
				continue;
			}
			if(isWithFlank) {
				while((line = br.readLine()) != null) {
					splitArray = line.split("\t");
					skipLine = parseBedFileLine(splitArray, flankRegion);
					if(skipLine) {
						continue;
					}
					processWindow();
				}
				//处理最后一个窗口
				processWindow(start, end, chrName);
			}
		}
		
		br.close();
	}
}
