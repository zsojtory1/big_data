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
		
		//create combined map initially with the elements of terms1
		Map<String,Short> terms3 = new HashMap<>(terms1);
		
		//Combine terms1 and terms2 into terms3
		for (Map.Entry<String, Short> entry : terms2.entrySet()) {
		    String term = entry.getKey();
		    Short value = entry.getValue();
		    terms3.compute(term, (k, v) -> (v == null) ? value : (short)(v + value));
		}
		
		CleanedArticle article = new CleanedArticle(null, terms3);
		
		return article;
	}
	
	
	
}
