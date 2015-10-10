import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;


public final class KMeansMP {
	
	private static class ParsePoint implements Function<String, Vector> {
		
		private static final Pattern spacechar = Pattern.compile(",");
		
		public Vector call(String line) {
			String[] token = spacechar.split(line);
			
			double[] point = new double[token.length-1];
			
			for (int i = 1; i < token.length; ++i) {
				
				point[i-1] = Double.parseDouble(token[i]);
			}
			return Vectors.dense(point); 
		}
		
	}
	
	private static class ParseTitle implements Function<String, String> {
		
		private static final Pattern spacechar = Pattern.compile(",");
		
		public String call(String line) {
			String[] token = spacechar.split(line);
			
			return token[0];
		}
	}
	
	private static class PrintCluster implements VoidFunction<Tuple2<Integer, Iterable<String>>> {
		private KMeansModel model;
		
		public PrintCluster (KMeansModel model) {
			this.model = model;
		}
		
		public void call(Tuple2<Integer, Iterable<String>> Cars) throws Exception {
			String ret = "[";
			
			for(String car: Cars._2()) {
				ret += car + ", ";
			}
			
			System.out.println(ret + "]");
		}
	}
	
	
	
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println(
                    "Usage: KMeansMP <input_file> <results>");
            System.exit(1);
        }
        String inputFile = args[0];
        String results_path = args[1];
        JavaPairRDD<Integer, Iterable<String>> results;
        int k = 4;
        int iterations = 100;
        int runs = 1;
        long seed = 0;
		final KMeansModel model;
		
        SparkConf sparkConf = new SparkConf().setAppName("KMeans MP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //TODO

        results.saveAsTextFile(results_path);

        sc.stop();
    }
}