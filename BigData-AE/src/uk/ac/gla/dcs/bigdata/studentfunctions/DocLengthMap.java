package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;

/**
 * @author zoltan
 * Map each cleaned article to its length
 */
public class DocLengthMap implements MapFunction<CleanedArticle, Integer> {
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public Integer call(CleanedArticle article) throws Exception {
		// TODO Auto-generated method stub
		return article.getDocLength();
	}
	
}
