package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * Extracts the MetaCritic score for a game (SteamGameStats object)
 * @author Zoltan
 *
 */
public class SortResults implements MapFunction<DocumentRanking, DocumentRanking> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public DocumentRanking call(DocumentRanking documentRanking) throws Exception {
        List<RankedResult> sortedResults = documentRanking.getResults().stream()
                .sorted((result1, result2) -> Double.compare(result2.getScore(), (result1.getScore())))
                .limit(20)
                .collect(Collectors.toList());

        return new DocumentRanking(documentRanking.getQuery(), sortedResults);
    }
}
