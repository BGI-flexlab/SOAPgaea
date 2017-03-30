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
 * Copyright (C)  2016  Pablo Cingolani(pcingola@users.sourceforge.net)
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.bgi.flexlab.gaea.tools.annotator.effect;

import htsjdk.variant.variantcontext.VariantContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 * Annotate a VCF entry
 *
 */
public class VcfAnnotator implements Serializable{
	
	private static final long serialVersionUID = -6632995517658045554L;
	
	Config config;
	SnpEffectPredictor snpEffectPredictor;
	VariantContext variantContext;
	
	public VcfAnnotator(Config config){
		this.config = config;
		snpEffectPredictor = config.getSnpEffectPredictor();
	}
	
	/**
	 * Annotate a VCF entry
	 *
	 * @return true if the entry was annotated
	 */
	public boolean annotate(VcfAnnotationContext vac) {
		List<AnnotationContext> annotationContexts = new ArrayList<AnnotationContext>();
//		HashMap<String, AnnotationContext> annotationContexts = new HashMap<String, AnnotationContext>();
		
//		boolean filteredOut = false;
		//---
		// Analyze all changes in this VCF entry
		// Note, this is the standard analysis.
		//---
		List<Variant> variants = vac.variants(config.getGenome());
		for (Variant variant : variants) {
			// Calculate effects: By default do not annotate non-variant sites
			if (variant.isVariant()) {
				VariantEffects variantEffects = snpEffectPredictor.variantEffect(variant);
				for (VariantEffect variantEffect : variantEffects) {
					AnnotationContext annotationContext = new AnnotationContext(variantEffect);
					annotationContexts.add(annotationContext);
				}
			}
		}
		
		if (annotationContexts.isEmpty()) return false;
		
		vac.setAnnotationContexts(annotationContexts);
		
		return true;
	}
	
	public List<String> convertAnnotationStrings(VcfAnnotationContext vac) {
		
		List<String> annoStrings = new ArrayList<String>();
		for(AnnotationContext ac : vac.getAnnotationContexts()){
			
			StringBuilder sb = new StringBuilder();
			sb.append(vac.getContig());
			sb.append("\t");
			sb.append(vac.getStart());
			sb.append("\t");
			sb.append(vac.getReference().getBaseString());
			sb.append("\t");
//			sb.append(ac.getFieldByName("ALLELE"));
			sb.append(ac.getAllele());
			String[] fields = config.getFieldsByDB(Config.KEY_GENE_INFO);
			for (String field : fields) {
				sb.append("\t");
				if(!ac.getFieldByName(field).isEmpty()){
					sb.append(ac.getFieldByName(field));
				}else {
					sb.append(".");
				}
			}
			
			List<String> dbNameList = config.getDbNameList();
			for (String dbName : dbNameList) {
				fields = config.getFieldsByDB(dbName);
				for (String field : fields) {
					sb.append("\t");
//						System.err.println("getNumAnnoItems:"+annoContext.getNumAnnoItems());
					sb.append(ac.getAnnoItemAsString(field, ".")); 
				}
			}
			annoStrings.add(sb.toString());
		}
		return annoStrings;
	}
	
	public VariantContext convertAnnotationVcfline(VcfAnnotationContext vac) {
//		TODO
		
		return null;
	}

}
