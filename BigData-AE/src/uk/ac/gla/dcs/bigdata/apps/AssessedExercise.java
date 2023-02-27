package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.CleanedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.HashMapAccumulator;
/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		//Custom accumulator for counting document-term frequencies across the corpus
		HashMapAccumulator corpusCounter = new HashMapAccumulator();
		spark.sparkContext().register(corpusCounter);
		
		//Preprocess articles (remove stopwords, stemming, etc.), giving a dataset of cleaned articles
		PreprocessArticle preprocessArticleMap = new PreprocessArticle(corpusCounter);
		Dataset<CleanedArticle> cleanedArticles = news.map(preprocessArticleMap, Encoders.bean(CleanedArticle.class));
		
		//Find the lengths of each document
		DocLengthMap docLenMap = new DocLengthMap();
		Dataset<Integer> docLens = cleanedArticles.map(docLenMap, Encoders.INT());
		
		//Use the total length of all documents and the number of documents to calculate the average document length
		SumDocLengthReducer sumDocLength = new SumDocLengthReducer();
		Integer totalDocLen = docLens.reduce(sumDocLength);
		long docsCount = docLens.count();
		Double avgDocLen = ((double)totalDocLen)/ docsCount;
		
		//Broadcast queries and the corpus to be used in the document ranking function
		List<Query> queriesList = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queriesList);
		Broadcast<HashMapAccumulator> broadcastCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(corpusCounter);
		
		//Rank each article with each query
		RankFlatMap rankFlatMap = new RankFlatMap(broadcastQueries, broadcastCorpus, avgDocLen, docsCount);
		Dataset<DocumentRanking> rankedDocs = cleanedArticles.flatMap(rankFlatMap, Encoders.bean(DocumentRanking.class));
		
		//Group the resulting dataset by query and convert it to a dataset of tuple 2
		KeyValueGroupedDataset<String,DocumentRanking> rankedDocsByQuery = rankedDocs.groupByKey(new DocumentRankingToQuery(), Encoders.STRING());
		Dataset<Tuple2<String, DocumentRanking>> ranksByQuery = rankedDocsByQuery.reduceGroups(new MergeDocRankingReducer());
		
		
		//Take only the document rankings from the tuple2, giving a dataset holding the unfiltered document rankings
		Dataset<DocumentRanking> ranks = ranksByQuery.map(new QueryDocRankTupleToDocRank(), Encoders.bean(DocumentRanking.class));
		
		//Sort results in each DocumentRanking based on score with a 20 article limit, then remove redundant results with a 10 article limit
		
		Dataset<DocumentRanking> reducedRanks = ranks.map(new RemoveRedundancy(), Encoders.bean(DocumentRanking.class));
		
		//Collect document rankings as list
		
		List<DocumentRanking> reducedRanksList = reducedRanks.collectAsList();
		
		return reducedRanksList; // return final result
	}
	
	
}
