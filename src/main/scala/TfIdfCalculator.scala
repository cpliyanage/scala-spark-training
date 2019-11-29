import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Created by akash on 2/25/19.
  */
object TfIdfCalculator {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //Read data from text files to Line RDD
    val linesRDD: RDD[String] = sc.textFile(sourceFilePath)

    // Split each text by space and create a list of words
    val splitText: RDD[List[String]] = splitTextBySpace(linesRDD)

    //create a dataframe from splitted documents
    val documents: DataFrame = splitText.toDF
    //Count tottal number of documents in the cropus
    val docCount: Double = documents.count()

    //Add a unique ID to each document and change the name of value column to document
    val dfWithDocId = documents.withColumn(docID, monotonically_increasing_id()).withColumnRenamed(valueDoc, document)
    //dfWithDocId.show()

    val columns: Array[Column] = dfWithDocId.columns.map(col) :+
      (explode(col(document)) as token)
    //for (word <- columns) println(word)


    //seperate tokens from documents
    val unfoldedDocs: DataFrame = dfWithDocId.select(columns: _*)
    //unfoldedDocs.show()

    //calculate the term frequency by grouping data usin doi id and token

    val tremFrequency: DataFrame = unfoldedDocs.groupBy(docID, token)
                                                  .agg(count(document) as termFrequency)
    //tremFrequency.show()

    //Calculate dofcument frequency
    val documentFreq = tremFrequency.groupBy(token)
                                  .agg(countDistinct(docID) as documentFrequency)
    //documentFrequency.show()

    val calcIdfUdf = udf { df: Long => math.log((docCount.toDouble + 1) / (df.toDouble + 1)) }
    //calculate IDF for each distinct token
    val IDF: DataFrame = documentFreq.withColumn(idf, calcIdfUdf(col(documentFrequency)))
    //IDF.show()

    //Join two dataframes. tf and Idf
    val TF_IDF = tremFrequency.join(IDF, Seq(token), "left").withColumn(tfIdf, col(termFrequency) * col(idf))
    //TF_IDF.orderBy(col("tf_idf").asc).show(20)


    //Get average TF_IDF value for a unique token in the cropus
    val stopList = TF_IDF.groupBy(col(token)).agg(sum(col(tfIdf))/ count(col(token))).orderBy(col("(sum(tf_idf) / count(token))").asc).limit(200)
                                .select(token).collect().map(_(0)).toList

    val stopListS = TF_IDF.groupBy(col(token)).agg(sum(col(tfIdf))/ count(col(token))).orderBy(col("(sum(tf_idf) / count(token))").asc).limit(200)
      .select(token)
    stopListS.write.text("/tmp/Akash/CDAP/stoplist.txt")

//    val stopList: List[Any] = TF_IDF.select("token").collect().map(_(0)).toList
//    stopList.foreach(println)




  }

  def splitTextBySpace (linesRDD: RDD[String]): RDD[List[String]] = {
    linesRDD.map(x => x.split("\\s+").map(_.trim).toList)
  }

  val sourceFilePath = "in/Test"        //"in/Test"
  val docID = "doc_id"
  val valueDoc = "value"
  val document = "document"
  val token = "token"
  val termFrequency = "termFrequency"
  val documentFrequency = "documentFrequency"
  val idf ="idf"
  val tfIdf = "tf_idf"

}
