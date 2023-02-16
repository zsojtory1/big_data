package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;

public class PreprocessArticle implements MapFunction<NewsArticle, CleanedArticle>{

	private static final long serialVersionUID = 6475166483071609772L;
	
	Broadcast<TextPreProcessor> broadcastTextPreprocessor;
	
	public PreprocessArticle(Broadcast<TextPreProcessor> broadcastTextPreprocessor) {
		this.broadcastTextPreprocessor = broadcastTextPreprocessor;
	}

	@Override
	public CleanedArticle call(NewsArticle article) throws Exception {
		
		String text = article.getTitle();
	
		int count = 0;
		for(ContentItem item: article.getContents()) {
			if ( count >= 5 ) {
				break;
			}
			
			String subtype = item.getSubtype();
			if ( subtype != null && subtype.equals("paragraph") ) {
				text += "\n" + item.getContent();
				count++;
			}
		}
		
		List<String> terms = this.broadcastTextPreprocessor.value().process(text);
		
		Map<String,Integer> termsMap = new HashMap<>();
		for(String term: terms) {
			if ( termsMap.containsKey(term) ) {
				termsMap.put(term, termsMap.get(term) + 1);
			}
			else {
				termsMap.put(term, 1);
			}
		}
		
		CleanedArticle cleanedArticle = new CleanedArticle(article, termsMap);
		
		return cleanedArticle;
	}
	
}
