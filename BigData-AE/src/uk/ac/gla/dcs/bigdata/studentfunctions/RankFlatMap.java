package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.HashMapAccumulator;

/**
 * @author zoltan
 * Flat map function, calculating the scores for each cleaned article against each query, returning a document ranking.
 */
public class RankFlatMap implements FlatMapFunction<CleanedArticle, DocumentRanking> {
	
	private static final long serialVersionUID = 6475166483071609772L;
	
	Broadcast<List<Query>> broadcastQueries;
	Broadcast<HashMapAccumulator> broadcastCorpus;
	Double avgDocLen;
	long docCount;
	
	public RankFlatMap(
			Broadcast<List<Query>> broadcastQueries, 
			Broadcast<HashMapAccumulator> broadcastCorpus, 
			Double avgDocLen,
			long docsCount
			) {
		this.broadcastQueries = broadcastQueries;
		this.broadcastCorpus = broadcastCorpus;
		this.avgDocLen = avgDocLen;
		this.docCount = docsCount;
	}

	@Override
	public Iterator<DocumentRanking> call(CleanedArticle cleanedArticle) throws Exception {
		
		List<Query> queries = this.broadcastQueries.value();
		HashMapAccumulator corpus = this.broadcastCorpus.value();
		
		Map<String,Integer> corpusTermDict = corpus.value();
		Map<String,Short> docTermDict = cleanedArticle.getTerms();
		
		//create a list of rankings (one for each query)
		List<DocumentRanking> rankings = new ArrayList<>(queries.size());
		
		//loop through each query, calculating the average scores between the all the terms in the query and the article through DPH
		for(Query query: queries) {
			List<String> terms = query.getQueryTerms();
			
			double sumScore = 0;
			//check each term in both corpus and article document-term frequencies
			for(int i=0; i<terms.size(); ++i) {
				String term = terms.get(i);
				short tfInDoc = 0;
				if ( docTermDict.containsKey(term) ) {
					tfInDoc = docTermDict.get(term);
				}
				int tfInCorpus = 0;
				if ( corpusTermDict.containsKey(term) ) {
					tfInCorpus = corpusTermDict.get(term);
				}
				
				//calculate score using values from earlier in the pipeline
				double score = DPHScorer.getDPHScore(tfInDoc, tfInCorpus, cleanedArticle.getDocLength(), avgDocLen, docCount);
				if ( !Double.isNaN(score) ) {
					sumScore += score;
				}
			}
			
			//calculate average and create ranked result objects
			double avgScore = sumScore / terms.size();
			NewsArticle article = cleanedArticle.getArticle();
			RankedResult rankedResult = new RankedResult(article.getId(), article, avgScore);
			
			//store each ranked result
			List<RankedResult> results = new ArrayList<RankedResult>();
			results.add(rankedResult);
			
			//store each document ranking per query
			DocumentRanking ranking = new DocumentRanking(query, results);
			rankings.add(ranking);
		}
		
		//return each document ranking
		return rankings.iterator();
	}
	
}
