package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TermAggregator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -301096133485328937L;
	private Map<String, Short> corpus = new HashMap<>();

	public void addTerm(String term) {
		if (!corpus.containsKey(term)) {
			corpus.put(term, (short) 1);
		} else {
			corpus.put(term, (short) (corpus.get(term) + 1));
		}
	}

	public Map<String, Short> getCorpus() {
		return corpus;
	}
}