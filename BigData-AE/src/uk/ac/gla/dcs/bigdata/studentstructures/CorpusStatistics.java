package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class CorpusStatistics implements Serializable {
  
  /**
	 * 
	 */
	private static final long serialVersionUID = 8125393120328669668L;
	private String term;
	private long termFrequencyInCorpus;
	private long totalDocumentLength;
	private double averageDocumentLength;
	private long corpusSize;
  
  public CorpusStatistics() {}
  
  public CorpusStatistics(String term, long termFrequencyInCorpus, long totalDocumentLength,
                           double averageDocumentLength, long corpusSize) {
    this.term = term;
    this.termFrequencyInCorpus = termFrequencyInCorpus;
    this.totalDocumentLength = totalDocumentLength;
    this.averageDocumentLength = averageDocumentLength;
    this.corpusSize = corpusSize;
  }
  
  public String getTerm() {
    return term;
  }
  
  public void setTerm(String term) {
    this.term = term;
  }
  
  public long getTermFrequencyInCorpus() {
    return termFrequencyInCorpus;
  }
  
  public void setTermFrequencyInCorpus(long termFrequencyInCorpus) {
    this.termFrequencyInCorpus = termFrequencyInCorpus;
  }
  
  public long getTotalDocumentLength() {
    return totalDocumentLength;
  }
  
  public void setTotalDocumentLength(long totalDocumentLength) {
    this.totalDocumentLength = totalDocumentLength;
  }
  
  public double getAverageDocumentLength() {
    return averageDocumentLength;
  }
  
  public void setAverageDocumentLength(double averageDocumentLength) {
    this.averageDocumentLength = averageDocumentLength;
  }
  
  public long getCorpusSize() {
    return corpusSize;
  }
  
  public void setCorpusSize(long corpusSize) {
    this.corpusSize = corpusSize;
  }
  
  public double getInverseDocumentFrequency() {
    return Math.log10((corpusSize * 1.0) / termFrequencyInCorpus);
  }
  
}
