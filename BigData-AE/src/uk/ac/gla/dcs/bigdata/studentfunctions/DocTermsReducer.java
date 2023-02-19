package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;

public class DocTermsReducer implements ReduceFunction<CleanedArticle> {
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public CleanedArticle call(CleanedArticle a1, CleanedArticle a2) throws Exception {
	
		Map<String,Short> terms1 = a1.getTerms();
		Map<String,Short> terms2 = a2.getTerms();
		
		Map<String,Short> terms3 = new HashMap<>();
		
		terms3.putAll(terms1);
		
		for(String term: terms2.keySet()) {
			Short value = terms2.get(term);
			if ( terms3.containsKey(term) ) {
				terms3.put(term, (short) (terms3.get(term) + value));
			}
			else {
				terms3.put(term, value);
			}
		}
		
		CleanedArticle article = new CleanedArticle(null, terms3);
		
		return article;
	}
	
	
	
}
