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
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;

public class RankFlatMap implements FlatMapFunction<CleanedArticle, DocumentRanking> {
	
	private static final long serialVersionUID = 6475166483071609772L;
	
	Broadcast<List<Query>> broadcastQueries;
	Broadcast<DPHScorer> broadcastScorer;
	Broadcast<CleanedArticle> broadcastCorpus;
	Double avgDocLen;
	long docCount;
	
	public RankFlatMap(
			Broadcast<List<Query>> broadcastQueries, 
			Broadcast<CleanedArticle> broadcastCorpus, 
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
		CleanedArticle corpus = this.broadcastCorpus.value();
		
		Map<String,Short> corpusTermDict = corpus.getTerms();
		Map<String,Short> docTermDict = cleanedArticle.getTerms();
		
		List<DocumentRanking> rankings = new ArrayList<>(queries.size());
		
		for(Query query: queries) {
			List<String> terms = query.getQueryTerms();
			
			double sumScore = 0;
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
				
				double score = DPHScorer.getDPHScore(tfInDoc, tfInCorpus, cleanedArticle.getDocLength(), avgDocLen, docCount);
				if ( !Double.isNaN(score) ) {
					sumScore += score;
				}
			}
			
			double avgScore = sumScore / terms.size();
			NewsArticle article = cleanedArticle.getArticle();
			RankedResult rankedResult = new RankedResult(article.getId(), article, avgScore);
			
			List<RankedResult> results = new ArrayList<RankedResult>();
			results.add(rankedResult);
			
			DocumentRanking ranking = new DocumentRanking(query, results);
			rankings.add(ranking);
		}
		
		return rankings.iterator();
	}
	
}
