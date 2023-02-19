package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

public class DocumentRankingToQuery implements MapFunction<DocumentRanking, String> {
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public String call(DocumentRanking value) throws Exception {
		// TODO Auto-generated method stub
		return value.getQuery().getOriginalQuery();
	}

}
