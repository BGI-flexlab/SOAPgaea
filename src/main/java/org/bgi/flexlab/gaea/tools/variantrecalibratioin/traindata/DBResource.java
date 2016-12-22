package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.variant.variantcontext.VariantContext;

public class DBResource implements ResourceType{

	private KnownDatabase db;
	
	private int resourceId;
	
	@Override
	public void initialize(String reference, String dbSnp) {
		// TODO Auto-generated method stub
		try {
			db = new KnownDatabase();
			String[] lineSplits = dbSnp.split("-");
			ResultSet rs = db.getSourceId(lineSplits[0], lineSplits[1]);
	    	if(rs.next()) {
	    		resourceId = rs.getInt(1);
	    		ResourceTag.NAME.setProperty(lineSplits[1]);
	    		ResourceTag.KNOWN.setProperty(rs.getString(4));
	    		ResourceTag.TRAINING.setProperty(rs.getString(5));
	    		ResourceTag.ANTITRAINING.setProperty(rs.getString(6));
	    		ResourceTag.TRUTH.setProperty(rs.getString(7));
	    		ResourceTag.CONSENSUS.setProperty(rs.getString(8));
	    		if(Double.parseDouble(ResourceTag.PRIOR.getProperty()) == 0)
	    			ResourceTag.PRIOR.setProperty(rs.getString(9));
	    	}
	    	if(rs.next()) {
	    		throw new RuntimeException("more than one resource in name of " + ResourceTag.NAME.getProperty() + " under ref:" + lineSplits[0]);
	    	}
		} catch(ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ArrayList<VariantContext> get(GenomeLocation loc) {
		// TODO Auto-generated method stub
		ArrayList<VariantContext> vcl = db.getTrainData(resourceId, loc.getContig(), loc.getStart(), loc.getStop());
		if(vcl == null) {
			return new ArrayList<VariantContext>();
		}
		return vcl;
	}

}
