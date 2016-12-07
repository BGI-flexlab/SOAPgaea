package org.bgi.flexlab.gaea.data.structure.reference.index;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformation;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class vcfIndex extends referenceIndex {

	private void saveAsBinary(String outputPath, byte[] binaries) throws IOException {
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputPath)));
		out.write(binaries);
		out.close();
	}

	private void insertSNPInformation(byte[] sequences, int position, byte flag) {
		int index = position >> 2;

		int leftMove = Byte.SIZE - 2 - ((position & 0x3) * 2);

		sequences[index] |= (flag << leftMove);
	}

	@SuppressWarnings("resource")
	@Override
	protected void dbsnpParser(String dbsnpPath, String outputPath) {
		VCFLocalLoader reader = null;
		try {
			reader = new VCFLocalLoader(dbsnpPath);
		} catch (IOException e1) {
			throw new RuntimeException(e1.toString());
		}
		Iterator<VariantContext> iterator = reader.iterator();

		String lastChrName = null;
		int lastLength = -1;
		ChromosomeInformation curChrInfo = null;
		byte[] sequences = null;

		String dbsnpList = outputPath + "/dbsnp_bn.list";
		FileWriter bnListWriter = null;
		try {
			bnListWriter = new FileWriter(new File(dbsnpList));
		} catch (IOException e1) {
			throw new RuntimeException(e1.toString());
		}

		while (iterator.hasNext()) {
			VariantContext context = iterator.next();
			String chrName = context.getContig();

			if (lastChrName == null || !lastChrName.equals(chrName)) {
				curChrInfo = chromosomeInfoMap.get(chrName);
				if (null == curChrInfo)
					throw new RuntimeException(
							"> Failed appending dbSNP information. No information related to chromosome name: "
									+ chrName);
				if (lastChrName != null && !lastChrName.equals(chrName)) {
					try {
						saveAsBinary(outputPath + "/" + lastChrName + ".dbsnp.bn", sequences);
					} catch (IOException e) {
						throw new RuntimeException(e.toString());
					}
					try {
						bnListWriter.write(chrName);
						bnListWriter.write("\t");
						bnListWriter.write(outputPath + "/" + lastChrName + ".dbsnp.bn");
						bnListWriter.write("\t");
						bnListWriter.write(String.valueOf(lastLength));
						bnListWriter.write("\n");
					} catch (IOException e) {
						throw new RuntimeException(e.toString());
					}

					sequences = null;
					sequences = new byte[(curChrInfo.getLength() + 3) / 4];
					Arrays.fill(sequences, 0, sequences.length, (byte) 0);
				}
				lastChrName = chrName;
				lastLength = curChrInfo.getLength();
			}

			for (int pos = context.getStart(); pos <= context.getEnd(); pos++) {
				if (pos < curChrInfo.getLength()) {
					curChrInfo.insertSnpInformation(pos - 1);
					byte flag = 0;
					if (context.getType() == Type.SNP)
						flag = 1;
					else if (context.getType() == Type.INDEL)
						flag = 2;
					else if (context.getType() == Type.MIXED)
						flag = 3;
					insertSNPInformation(sequences, pos - 1, flag);
				}
			}
		}

		reader.close();
		try {
			bnListWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}
}
