package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class CleanedArticle implements Serializable {
	
	private static final long serialVersionUID = -2905684103776472843L;
	
	Map<String,Short> terms;
	NewsArticle article;
	int docLength;
	
	public CleanedArticle(NewsArticle article, Map<String,Short> terms) {
		this.terms = terms;
		this.article = article;
		
		int docLength = 0;
		for(short count: terms.values()) {
			docLength += count;
		}
		this.docLength = docLength;
	}
	
	public int getDocLength() {
		return docLength;
	}
	
	public Map<String,Short> getTerms(){
		return terms;
	}
	
	public NewsArticle getArticle() {
		return article;
	}
}
