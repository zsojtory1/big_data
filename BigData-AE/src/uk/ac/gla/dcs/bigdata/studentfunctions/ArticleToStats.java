package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;


public class ArticleToStats implements FlatMapFunction<CleanedArticle, String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1588523753105562869L;

	@Override
	public Iterator<String> call(CleanedArticle preprocessedDoc) throws Exception {
        return preprocessedDoc.getTerms().iterator();
	}
}
