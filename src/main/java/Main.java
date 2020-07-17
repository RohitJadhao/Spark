import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import com.databricks.spark.avro.*;
import java.io.File;

public class Main {

    public static void main(String[] args){

        //SparkConf conf = new SparkConf().set("spark.testing.memory", "2147480000");
        /*JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("/user/rohitjadhao926566/shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("/user/rohitjadhao926566/shakespeareWordCount");*/
        try {
            Schema schemaAvro = new Schema.Parser().parse(new File(""));
        }catch(Exception e){
            System.out.println(e);
        }

        SparkSession spark = SparkSession.builder().master("local[*]").config("spark.testing.memory", "2147480000").getOrCreate();
        Dataset<Row> ds = spark.read().format("com.databricks.spark.avro").load("/user/rohitjadhao926566/episodes.avro");
        ds.write().mode(SaveMode.Append).format("com.databricks.spark.avro").save("/user/rohitjadhao926566/country_partition_userdata1");

    }
}
