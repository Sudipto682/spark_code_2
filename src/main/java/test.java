import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.util.parsing.combinator.testing.Str;

import java.security.Key;
import java.util.*;

/**
 * Created by ssah22 on 9/13/2017.
 */
public class test {

    public static void main(String args[])
    {

        SparkConf sparkConf = new SparkConf().setAppName("Educomp Services").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> q_data = javaSparkContext.textFile("C:\\Users\\ssah22\\Desktop\\edutech\\question_data.txt");
        JavaRDD<String> s_report = javaSparkContext.textFile("C:\\Users\\ssah22\\Desktop\\edutech\\student_consolidated_report.txt");
        JavaRDD<String> s_response = javaSparkContext.textFile("C:\\Users\\ssah22\\Desktop\\edutech\\student_response.txt");

        /*SQLContext sqlContext = new SQLContext(javaSparkContext);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL ")
                .getOrCreate();


        //generating the schema
        String schemaString1 = "QID QDesc Section Category Difficulty C_by C_date";
        List<StructField> fields = new ArrayList<>();
        for(String fieldname:schemaString1.split(" "))
        {
            StructField field = DataTypes.createStructField(fieldname,DataTypes.StringType,true);
            fields.add(field);
        }

        StructType schema1 = DataTypes.createStructType(fields);


        JavaRDD<Row> rowRdd1 = q_data.map(x->{
            String[] attributes = x.split("\\|");
             return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4],attributes[5],attributes[6]);
        });

        Dataset<Row> question_data = spark.createDataFrame(rowRdd1,schema1);
        question_data.createOrReplaceTempView("q_data");
        Dataset<Row> q1= spark.sql("select * from q_data");
        q1.show();

*//*
        String schemaString2 = "S_id Q_set QT VB RA Attempt_q Attempt_date";
        List<StructField> fields2 = new ArrayList<>();
        for(String fieldname:schemaString2.split(" "))
        {
            StructField f = DataTypes.createStructField(fieldname,DataTypes.StringType,true);
            fields2.add(f);
        }

        StructType schema2 = DataTypes.createStructType(fields2);*//*
        StructType schema2 = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("S_id", DataTypes.StringType, false),
                        DataTypes.createStructField("Q_set", DataTypes.StringType, false),
                        DataTypes.createStructField("QT", DataTypes.IntegerType, true),
                        DataTypes.createStructField("VB", DataTypes.IntegerType, true),
                        DataTypes.createStructField("RA", DataTypes.IntegerType, true),
                        DataTypes.createStructField("Attempt_q", DataTypes.IntegerType, true),
                        DataTypes.createStructField("Attempt_date", DataTypes.StringType, true)});

        JavaRDD<Row> rowRdd2 = s_report.map(x->{
            String[] attributes = x.split("\\|");
            return RowFactory.create(attributes[0],attributes[1],Integer.parseInt(attributes[2]),Integer.parseInt(attributes[3]),Integer.parseInt(attributes[4]),Integer.parseInt(attributes[5]),attributes[6]);
        });

        Dataset<Row> student_report = spark.createDataFrame(rowRdd2,schema2);
        student_report.createOrReplaceTempView("Student_report");
        Dataset<Row> q2 = spark.sql("select * from Student_report");
        q2.show();
        student_report.groupBy("Q_set").avg().show();
*/


        JavaPairRDD<String, Tuple3<Integer,Integer,Integer>> d1 = s_report.mapToPair(x->{
            String[] attributes = x.split("\\|");
            Tuple3<Integer,Integer,Integer> t = new Tuple3<>(Integer.parseInt(attributes[2]),Integer.parseInt(attributes[3]),Integer.parseInt(attributes[4]));
            return new Tuple2<>(attributes[1],t);

        });


        JavaPairRDD<String, Tuple4<Integer,Integer,Integer,Integer>> d2 = d1.mapValues(x->{
            return new Tuple4<>(x._1(),x._2(),x._3(),1);

        });


        JavaPairRDD<String,Tuple4<Integer,Integer,Integer,Integer>> d3 = d2.reduceByKey((t1,t2)->new Tuple4<>(t1._1()+t2._1(),t1._2()+t2._2(),t1._3()+t2._3(),t1._4()+t2._4()));
        //JavaPairRDD<String,Float> r1 = d3.mapValues(x-> x._1()/x._4());
        JavaPairRDD<String,Tuple3<Integer,Integer,Integer>> r1 = d3.mapValues(x->{
            return new Tuple3<>(x._1()/x._4(),x._2()/x._4(),x._3()/x._4());
        });
        d3.foreach(x->{
            System.out.println(x._1+"*****"+x._2);
        });

        r1.foreach(x->{
            System.out.println(x._1+"*****"+x._2);
        });

        JavaPairRDD<Integer,Tuple4<String,Integer,Integer,Integer>> s1 = s_report.mapToPair(x->{
            String[] attributes = x.split("\\|");
            Tuple4<String,Integer,Integer,Integer> t1 = new Tuple4<>(attributes[1],Integer.parseInt(attributes[2]),Integer.parseInt(attributes[3]),Integer.parseInt(attributes[4]));
            return new Tuple2<>(Integer.parseInt(attributes[0]),t1);
        });

        Map<String,Tuple3<Integer,Integer,Integer>> map = r1.collectAsMap();
        HashMap<String,Tuple3<Integer,Integer,Integer>> hmap = new HashMap<>(map);

        Map<Integer,Tuple4<String,Integer,Integer,Integer>> s_map = s1.collectAsMap();
        HashMap<Integer,Tuple4<String,Integer,Integer,Integer>> s_hmap = new HashMap<>(s_map);

       for(Map.Entry<Integer,Tuple4<String,Integer,Integer,Integer>> entry1:s_map.entrySet() )
       {

           Tuple4<String,Integer,Integer,Integer> s_value = entry1.getValue();
           for(Map.Entry<String,Tuple3<Integer,Integer,Integer>> entry2 : hmap.entrySet())
           {
               String q_set= entry2.getKey();
               Tuple3<Integer,Integer,Integer> avg = entry2.getValue();
               if(q_set.equals(s_value._1()))
               {
                   if(s_value._2()<avg._1())
                       System.out.println(entry1.getKey()+" got less in section A,student marks="+s_value._2()+" average marks="+avg._1());
                   else if(s_value._3()<avg._2())
                       System.out.println(entry1.getKey()+" got less in section B,student marks="+s_value._3()+" average marks="+avg._2());
                   else if(s_value._3()<avg._2())
                       System.out.println(entry1.getKey()+" got less in section C,student marks="+s_value._3()+" average marks="+avg._3());
                   else
                       System.out.println(entry1.getKey()+" got more than average");

               }




           }
       }

        s1.foreach(x->{
            System.out.println(x._1+"*****"+x._2);
        });

       JavaPairRDD<String,Tuple2<String,String>> s_rdd = s_response.mapToPair(x->{
           String[] attributes = x.split("\\|");
           return new Tuple2<>(attributes[3],new Tuple2<>(attributes[4],attributes[5]));
       });

       JavaPairRDD<String,Tuple2<String,String>> s_rdd_filtered = s_rdd.filter(x->{
           if(x._1().equals('Y'))
           {
               if(x._2().equals('R'))
                   return new Tuple2<>()
           }
       })

       s_rdd.foreach(x->{
           System.out.println(x._1()+"-----------"+x._2());
       });









    }
}
