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
package org.bgi.flexlab.gaea.tools.annotator;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  The special annotation context for each sample
 */
public class SampleAnnotationContext{

	private String sampleName;
	private List<String> alts;   // variants at chr:pos
	private int depth;
	private Map<String, Integer> alleleDepths = null;
	private Map<String, String> alleleRatios = null;
	private Map<String, String> zygosity = null;
	private Map<String, String> filter = null;
	private boolean hasNearVar = false;
	private String singleAlt;

	public SampleAnnotationContext() {

	}

	public SampleAnnotationContext(String sampleName) {
		this.sampleName = sampleName;
	}

	public String getFieldByName(String fieldName, String allele) {
		switch (fieldName) {
			case "NbGID":
				if(hasNearVar) return "1";
				else return "0";

			case "A.Depth":
				return Integer.toString(getAlleleDepth(allele));

			case "A.Ratio":
				return getAlleleRatio(allele);

			case "Zygosity":
				return getAlleleZygosity(allele);

			case "Filter":
				return ".";

			default:
				return null;
		}
	}

	public String getSampleName() {
		return sampleName;
	}

	public void setSampleName(String sampleName) {
		this.sampleName = sampleName;
	}

	public boolean isHasNearVar() {
		return hasNearVar;
	}

	public void setHasNearVar() {
		this.hasNearVar = true;
	}

	private void setAlleleRatios(){
		DecimalFormat df = new DecimalFormat("0.00");
		df.setRoundingMode(RoundingMode.HALF_UP);
		alleleRatios = new HashMap<>();
		for(String alt: getAlts()){
			double ratio = getDepth() == 0 ? 0 : getAlleleDepth(alt)*1.0 / getDepth();
			alleleRatios.put(alt, df.format(ratio));
		}
	}

	public String getAlleleZygosity(String allele) {
		return zygosity.get(allele);
	}

	public Map<String, String> getZygosity() {
		return zygosity;
	}

	public void setZygosity(Map<String, String> zygosity) {
		this.zygosity = zygosity;
	}

	public int getDepth() {
		return depth;
	}

	public void setDepth(int depth) {
		this.depth = depth;
	}

	public Map<String, Integer> getAlleleDepths() {
		return alleleDepths;
	}

	public void setAlleleDepths(Map<String, Integer> alleleDepths) {
		this.alleleDepths = alleleDepths;
	}

	public List<String> getAlts() {
		return alts;
	}

	public void setAlts(List<String> alts) {
		this.alts = alts;
	}

	public String getSingleAlt() {
		return singleAlt;
	}

	public boolean hasAlt(String alt){
		return alts.contains(alt);
	}

	public String getAlleleRatio(String allele){
		return alleleRatios.get(allele);
	}

	public int getAlleleDepth(String allele){
		return alleleDepths.get(allele);
	}

	public String toAlleleString(String allele){
		if(null == alleleRatios)
			setAlleleRatios();
		StringBuilder sb = new StringBuilder();
		sb.append(getSampleName());
		sb.append("|");
		sb.append(allele);
		sb.append("|");
		sb.append(getAlleleRatio(allele));
		sb.append("|");
		sb.append(getAlleleDepth(allele));
		sb.append("|");
		sb.append(isHasNearVar() ? 1 : 0);
		return sb.toString();
	}

	public void parseAlleleString(String alleleString){
		String[] fields = alleleString.split("\\|");
		System.err.println("SI:"+alleleString);
		System.err.println("Fields:"+fields[0]);
		setSampleName(fields[0]);
		singleAlt = fields[1];
		alleleRatios = new HashMap<>();
		alleleDepths = new HashMap<>();
		alleleRatios.put(singleAlt, fields[2]);
		alleleDepths.put(singleAlt, Integer.parseInt(fields[3]));
		if(fields[4].equals("1"))
			setHasNearVar();
	}

}
