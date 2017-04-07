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
 *
 * This file incorporates work covered by the following copyright and 
 * Permission notices:
 *
 * Copyright (c) 2009-2012 The Broad Institute
 *  
 *     Permission is hereby granted, free of charge, to any person
 *     obtaining a copy of this software and associated documentation
 *     files (the "Software"), to deal in the Software without
 *     restriction, including without limitation the rights to use,
 *     copy, modify, merge, publish, distribute, sublicense, and/or sell
 *     copies of the Software, and to permit persons to whom the
 *     Software is furnished to do so, subject to the following
 *     conditions:
 *  
 *     The above copyright notice and this permission notice shall be
 *     included in all copies or substantial portions of the Software.
 *  
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 *     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *     NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 *     HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 *     WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *     FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 *     OTHER DEALINGS IN THE SOFTWARE.
 *******************************************************************************/
package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata;

import java.util.ArrayList;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.variant.variantcontext.VariantContext;

public class TrainData {	
	
	private String contextName;
	
	private ResourceType ram;
	
	private String ref;
	
	/**
	 * default constructor
	 */
	public TrainData(String ref, String resource) {
		this.ref = ref;
		ResourceTag.parseTag(resource);
		contextName = (isDB() ? ResourceTag.valueOf("DB").getProperty(): ResourceTag.valueOf("FILE").getProperty());
	}

	public void setType(ResourceType ram) {
		this.ram = ram;
	}
	
	public void initialize() {
		ram.initialize(ref, contextName);
	}
	
	public ArrayList<VariantContext> get(GenomeLocation loc) {
		// TODO Auto-generated method stub
		return ram.get(loc);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(ResourceTag tag : ResourceTag.values()) {
			sb.append(tag.getProperty());
			sb.append("\t");
		}
		return sb.toString();
	}
	
	public Object clone() {  
		TrainData o = null;  
        try {  
            o = (TrainData) super.clone();  
        } catch (CloneNotSupportedException e) {  
            e.printStackTrace();  
        }  
        return o; 
    }  
	
	/**
	 * @return the name
	 */
	public String getName() {
		return ResourceTag.valueOf("NAME").getProperty();
	}

	/**
	 * @return the isKnown
	 */
	public boolean isKnown() {
		return Boolean.parseBoolean(ResourceTag.valueOf("KNOWN").getProperty());
	}

	/**
	 * @return the isTraining
	 */
	public boolean isTraining() {
		return Boolean.parseBoolean(ResourceTag.valueOf("TRAINING").getProperty());
	}

	/**
	 * @return the isAntiTraining
	 */
	public boolean isAntiTraining() {
		return Boolean.parseBoolean(ResourceTag.valueOf("ANTITRAINING").getProperty());
	}

	public boolean isDB() {
		return ResourceTag.valueOf("DB").getProperty() != null;
	}
	
	/**
	 * @return the isTruth
	 */
	public boolean isTruth() {
		return Boolean.parseBoolean(ResourceTag.valueOf("TRUTH").getProperty());
	}

	/**
	 * @return the isConsensus
	 */
	public boolean isConsensus() {
		return Boolean.parseBoolean(ResourceTag.valueOf("CONSENSUS").getProperty());
	}

	/**
	 * @return the prior
	 */
	public double getPrior() {
		return Double.parseDouble(ResourceTag.valueOf("PRIOR").getProperty());
	}

	public String getContextName() {
		return contextName;
	}

}
