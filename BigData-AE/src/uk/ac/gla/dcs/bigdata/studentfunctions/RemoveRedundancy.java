package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/**
 * Extracts the MetaCritic score for a game (SteamGameStats object)
 * @author Zoltan
 *
 */
public class RemoveRedundancy implements MapFunction<DocumentRanking, DocumentRanking> {
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
        
        List<RankedResult> filteredResults = new ArrayList<>();
        
        for (int i = 0; i < sortedResults.size(); i++) {
            boolean keep = true;
            RankedResult result1 = sortedResults.get(i);
            for (int j = i + 1; j < sortedResults.size(); j++) {
                RankedResult result2 = sortedResults.get(j);
                if (TextDistanceCalculator.similarity(result1.getArticle().getTitle(), result2.getArticle().getTitle()) < 0.5) {
                    if (result1.getScore() < result2.getScore()) {
                        keep = false;
                        break;
                    }
                }
            }
            if (keep) {
                filteredResults.add(result1);
            }
        }

        return new DocumentRanking(documentRanking.getQuery(), filteredResults.stream().limit(10).collect(Collectors.toList()));
    }
}