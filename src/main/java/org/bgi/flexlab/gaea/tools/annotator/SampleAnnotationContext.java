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

import java.util.List;
import java.util.Map;

/**
 *  The special annotation context for each sample
 */
public class SampleAnnotationContext{

	private String sampleName;
	private List<String> alts;   // variants at chr:pos
	private int depth;
	private Map<String, Integer> alleleDepths;
	private boolean hasNearVar = false;

	public SampleAnnotationContext(String sampleName) {
		this.sampleName = sampleName;
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
}
