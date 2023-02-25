package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * @author zoltan
 * Combine document rankings which belong to the same query
 */
public class MergeDocRankingReducer implements ReduceFunction<DocumentRanking> {
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public DocumentRanking call(DocumentRanking dr1, DocumentRanking dr2) throws Exception {
		// TODO Auto-generated method stub
		List<RankedResult> results1 = dr1.getResults();
		List<RankedResult> results2 = dr2.getResults();
		
		List<RankedResult> results3 = new ArrayList<>();
		results3.addAll(results1);
		results3.addAll(results2);
		
		return new DocumentRanking(dr1.getQuery(), results3);
	}


}
