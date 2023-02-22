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
	private transient TextPreProcessor processor;
	
//	Broadcast<TextPreProcessor> broadcastTextPreprocessor;
	
	public PreprocessArticle() {
		this.processor = new TextPreProcessor();
	}
	
//	public PreprocessArticle(Broadcast<TextPreProcessor> broadcastTextPreprocessor) {
//		this.broadcastTextPreprocessor = broadcastTextPreprocessor;
//	}

	@Override
	public CleanedArticle call(NewsArticle article) throws Exception {
		
		if (processor==null) processor = new TextPreProcessor();
		
		String text = article.getTitle();
		if ( text == null ) {
			text = "";
		}
	
		int count = 0;
		for(ContentItem item: article.getContents()) {
			if ( item == null ) {
				continue;
			}
			
			if ( count >= 5 ) {
				break;
			}
			
			String subtype = item.getSubtype();
			if ( subtype != null && subtype.equals("paragraph") ) {
				text += "\n" + item.getContent();
				count++;
			}
		}
		
		List<String> terms = processor.process(text);
		
		//Create termsMap, mapping each term to its count in the article
		Map<String,Short> termsMap = new HashMap<>();
		for(String term: terms) {
		    termsMap.merge(term, (short) 1, (a, b) -> (short) (a + b));
		}
		
		CleanedArticle cleanedArticle = new CleanedArticle(article, termsMap);
		
		return cleanedArticle;
	}
	
}
