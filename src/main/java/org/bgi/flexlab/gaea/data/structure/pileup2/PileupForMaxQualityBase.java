package org.bgi.flexlab.gaea.data.structure.pileup2;

public class PileupForMaxQualityBase {
	public final static int MAX_READ_LENGTH = 75;

	protected int position;
	protected int currentElementIndex;

	protected PileupInformation[] pileupWindows = null;
	protected PileupInformation[] pileupBackupWindows = null;

	public PileupForMaxQualityBase() {
		position = Integer.MAX_VALUE;
		currentElementIndex = 0;
		initialize();
	}

	public void initialize() {
		pileupWindows = new PileupInformation[MAX_READ_LENGTH];
		pileupBackupWindows = new PileupInformation[MAX_READ_LENGTH];
		for (int i = 0; i < MAX_READ_LENGTH; i++) {
			pileupWindows[i] = null;
			pileupBackupWindows[i] = null;
		}
	}

	/**
	 * get sample plp
	 */
	public PileupInformation getPileup(int referencePosition){
		if(referencePosition < position)
			throw new RuntimeException("reference position must large than pileup position");
		int index = (referencePosition - position ) + currentElementIndex;
		return pileupWindows[index];
	}
	
	public PileupInformation getPileup(){
		return getPileup(this.position);
	}

	private void updateElement(PileupInformation[] pileups, int index,
			boolean insertion, boolean deletion) {
		if (pileups[index] == null) {
			pileups[index] = new PileupInformation(insertion, deletion);
		} else {
			pileups[index].add(insertion, deletion);
		}
	}

	private void updateElement(PileupInformation[] pileups, int index,
			byte base, byte quality) {
		if (pileups[index] == null) {
			pileups[index] = new PileupInformation(base, quality);
		} else {
			pileups[index].add(base, quality);
		}
	}

	private void calculatePileup(ReadInfo read) {
		int end = read.getEnd();
		for (int i = read.getPosition(); i <= end; i++) {
			int basePosition = read.getCigarState().resolveCigar(i,
					read.getPosition());
			boolean insertion = (basePosition == -2) ? true : false;
			boolean deletion = (basePosition == -1) ? true : false;

			int index = i - position + currentElementIndex;
			if (insertion || deletion) {
				if (index >= MAX_READ_LENGTH) {
					index -= MAX_READ_LENGTH;
					updateElement(pileupBackupWindows, index, insertion,
							deletion);
				} else {
					updateElement(pileupWindows, index, insertion, deletion);
				}
			} else {
				char b = read.getBaseFromRead(basePosition);
				if(b == 'N' || b == 'n')
					continue;
				byte base = (byte)((b>>1) & 0x3);
				byte quality = (byte)read.getBaseQuality(basePosition);
				if (index >= MAX_READ_LENGTH) {
					index -= MAX_READ_LENGTH;
					if(index >= MAX_READ_LENGTH){
						throw new RuntimeException("reads:"+read.getPosition()+"\t"+read.getEnd()+"\t"+read.getReadLength()+"\t"+read.getSample());
					}
					updateElement(pileupBackupWindows, index, base, quality);
				} else {
					updateElement(pileupWindows, index, base, quality);
				}
			}
		}
	}

	/**
	 * add readInfo to pileup
	 */
	public void addReads(ReadInfo read) {
		if (position >= read.getPosition() && position <= read.getEnd()) {
			calculatePileup(read);
		} else {
			throw new RuntimeException("an unexpection at pileup add reads ");
		}
	}

	public void forwardPosition(int size) {
		position += size;
		currentElementIndex += size;
		
		if(currentElementIndex >= MAX_READ_LENGTH){
			PileupInformation[] arrayTemp = pileupWindows;
			pileupWindows = pileupBackupWindows;
			pileupBackupWindows = arrayTemp;
			currentElementIndex -= MAX_READ_LENGTH;
			
			for(int i=0;i<MAX_READ_LENGTH;i++)
				pileupBackupWindows[i] = null;
		}
		
		if(isEmpty())
			position = Integer.MAX_VALUE;
	}

	public boolean isEmpty() {
		int i;
		for (i = currentElementIndex; i < MAX_READ_LENGTH; i++) {
			if(pileupWindows[i] != null ){
				PileupInformation pileup = pileupWindows[i];
				if(pileup.isSNP() || pileup.isDeletion() || pileup.isInsertion()){
					return false;
				}
			}
		}
		for (i = 0; i < MAX_READ_LENGTH; i++) {
			if(pileupBackupWindows[i] != null ){
				PileupInformation pileup = pileupBackupWindows[i];
				if(pileup.isSNP() || pileup.isDeletion() || pileup.isInsertion()){
					return false;
				}
			}
		}
		return true;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public void addPosition(int add) {
		this.position = position + add;
	}
}

