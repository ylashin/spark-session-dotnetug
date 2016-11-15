// Any text file can be used here
// you can grab Frankenstein novel from project Gutenberg site

val wordCounts = sc.textFile("d:\\files\\Frankenstein.txt")
                       .flatMap( a => a.split(" ") )
					   .filter( a=> a.length >= 5)
                       .map( a => (a,1) )
                       .reduceByKey( _ + _ )
                       .sortBy(a => a._2 , false)
                       .take(100)

					   
==========================================================================
// RDD concepts
// Parsing IIS log files -- grab any logs from an IIS server you have access to :)
val lines = sc.textFile("D:\\files\\Logs\\*").cache();

lines.count;

val filtered = lines.filter(a=> !a.startsWith("#"));

val statusCodes = filtered.map(a=> a.split(" ")).map(a => a(15).toInt);

statusCodes.take(10);

val counts = statusCodes.countByValue();

//export result to text file or Hive , etc
sc.makeRDD(counts.toList).saveAsTextFile("d:\\files\\iis-logs-aggregation");			

=====================================================================
// Car crashes
// search in [Azure data market places](https://datamarket.azure.com/browse/data) for **USA car crash 2011**
// Then import it in a SQL server instance and update the below connection string details
spark-shell -deprecation --driver-class-path c:\windows\system32\sqljdbc42.jar --jars c:\windows\system32\sqljdbc42.jar

val jdbcDF = spark.read.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;databaseName=CarCrash;")
    .option("dbtable", "(SELECT Id,Injury_Severity, Age,State  FROM Crashes) AS T")
    .option("user", "sqluser").option("password", "sqlpassword")
    .option("partitionColumn","Id")
    .option("lowerBound",1).option("upperBound", 72310)
    .option("numPartitions", 8).load();
    
    jdbcDF.toJavaRDD.partitions.size;   
    
    jdbcDF.filter("Injury_Severity = 'Fatal Injury (K)'").count();
	
    val fatalities = jdbcDF.filter("Injury_Severity = 'Fatal Injury (K)'").cache();	
    fatalities.filter("Injury_Severity = 'Fatal Injury (K)'").count(); // first call will cache
    fatalities.filter("Injury_Severity = 'Fatal Injury (K)'").count(); // no more SQL profiler hits :)
	
	
    fatalities.createOrReplaceTempView("people"); 
    spark.sql("SELECT State,count(*) AS Count from people GROUP BY State ORDER BY Count DESC").show();
	spark.sql("SELECT Age,count(*) AS Count from people GROUP BY Age ORDER BY Count DESC").show();
	
	
//==========================================================================================

//  Yammer 
// Presentation includes expected file formats
// You need to export it from your yammer network using REST API
// You can ping me on twitter @ylashin if you need a copy of source code but it should not be a big deal
import org.apache.spark.graphx._;
import org.apache.spark.rdd.RDD;
val people = sc.textFile("people.csv");
val connections = sc.textFile("connections.csv");

val vertices = people.map(x=> x.split(",")).map(x => (x(0).toLong, x(1)));
val defaultUser = ("Missing");
val edges = connections.map(x=> x.split(",")).map(x => Edge(x(0).toLong,x(1).toLong,x(2).toLong));
val graph = Graph(vertices, edges, defaultUser);
graph.persist();
graph.numVertices
graph.numEdges

// important people
val ranks = graph.pageRank(0.0001).vertices;
val rankedPeople = ranks.join(vertices).sortBy(_._2._1, ascending=false).map(_._2._2);
rankedPeople.take(10).foreach(println);

//People you may know
val personalisedRanks = graph.personalizedPageRank(1577284849, 0.001).vertices;
val personalisedRanksAndPeople = personalisedRanks.join(vertices).sortBy(_._2._1, ascending=false).map(_._2._2);
personalisedRanksAndPeople.take(20).foreach(println);

def toGexf[VD,ED](g:Graph[VD,ED]) =
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
"<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
" <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
" <nodes>\n" +
g.vertices.map(v => " <node id=\"" + v._1 + "\" label=\"" +
v._2 + "\" />\n").collect.mkString +
" </nodes>\n" +
" <edges>\n" +
g.edges.map(e => " <edge source=\"" + e.srcId +
"\" target=\"" + e.dstId + "\" label=\"" + e.attr +
"\" />\n").collect.mkString +
" </edges>\n" +
" </graph>\n" +
"</gexf>";
val pw = new java.io.PrintWriter("myGraph.gexf");
pw.write(toGexf(graph));
pw.close;
//======================================================================================
// Twitter Streaming
// Source code and instructions are in another [repo](https://github.com/ylashin/HadoopPD/blob/master/readme/SparkStreaming.md)
// The example in the demo was tweaked to filter incoming twitter stream for tweets with a certain keyword				

wget https://yousry1.blob.core.windows.net/public/sparky-azure.jar

spark-submit --class org.bigdata.sparky.trendingHashtags sparky-azure.jar

spark-submit --class org.bigdata.sparky.filteredTrendingHashtags sparky-azure.jar "dotnetbne"

// ======================================================================================
/// Movie Lens - Get the 20M dataset ratings/movie files from : http://grouplens.org/datasets/movielens/


import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import spark.implicits._;

case class Rating(userId: Int, movieId: Int, rating: Float);

def parseRating(str: String): Rating = {
    val fields = str.split(",");    
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat);
  };

 val ratings = spark.read.textFile("D:\\files\\ratings.csv").filter(s=> s(0).isDigit).map(parseRating).toDF().cache();
 ratings.count();


val als = new ALS().setMaxIter(10).setRegParam(0.1).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setRank(10);
val model = als.fit(ratings);

val userId = 18884;

case class Movie(movieId: Int, Name: String);

val allMovies = spark.read.textFile("D:\\files\\movies.csv")
                         .map(a => a.split(",")).map(a => Movie(a(0).toInt, a(1))).toDF();

allMovies.createOrReplaceTempView("movies");
ratings.createOrReplaceTempView("ratings");

spark.sql("select movies.Name, ratings.rating from ratings inner join movies on ratings.movieId = movies.movieId where userId = " + userId+ " order by ratings.rating desc").show();

//X-Men movies
var testMovies = sc.makeRDD(List(Rating(userId,68319,0),Rating(userId,87232,0), Rating(userId,111362,0)))
					
val predictionsForUser = model.transform(testMovies.toDF());

predictionsForUser.toDF().show();