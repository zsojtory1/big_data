package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

/**
 * @author zoltan
 * Map function, mapping a Tuple2<String, DocumentRanking> to just its second element which is Document Ranking
 */
public class QueryDocRankTupleToDocRank implements MapFunction<Tuple2<String,DocumentRanking>, DocumentRanking>{
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public DocumentRanking call(Tuple2<String, DocumentRanking> value) throws Exception {
		// TODO Auto-generated method stub
		return value._2;
	}

}
