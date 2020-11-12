package com.company.spark.recon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Test {

	private static JavaSparkContext javaSparkContext;

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Dev to Prod Application") //
				// .master("yarn-cluster")
				.master("local[*]")
				.config("spark.driver.bindAddress", "127.0.0.1")
//				.config("spark.sql.warehouse.dir", ConfigUtils.getStringProperty(ConfigUtils.SPARK_SQL_WAREHOUSE_DIR)) //
				// .enableHiveSupport()
				.getOrCreate();

		javaSparkContext = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> idcFile = javaSparkContext
				.textFile("C:\\Users\\Dell\\IdeaProjects\\recon\\data\\idc\\IDC-OBJECT_GROUP-Output-10.137.250.164_2020-10-30_11-45-01.txt");

		JavaRDD<String> removeEmptyRows = idcFile.filter(sentence -> sentence.trim().length() > 0);
		List<String> listOfWords = removeEmptyRows.collect();
		ArrayList<String> arraylistOfEqRows = new ArrayList<String>();
		String ipAddress = "";
		String parentNode = "";

		for(int i = 0; i<5; i++){
            String str = listOfWords.get(i);
            if(str.trim().contains("Succeeded")){
//                Oct-30-20 11:00:10	IDC-OBJECT_GROUP-Output-10.137.250.164	NVMBD1PS083R11PTF	10.137.250.164	Succeeded	3	Script '-- One-time use change plan -- for Cisco PIX enable' completed.
                String[] metdataArr = str.trim().split("\t");
                if(metdataArr.length >= 4 ) {
                    ipAddress = metdataArr[3];
					System.out.println("IpAddress available : " + ipAddress);
                } else {
                    System.out.println("IpAddress not available In valid file : " + str.trim());
                }
            }
        }
        for(int i = 4; i<listOfWords.size(); i++){
            String str = listOfWords.get(i);

			if(str.trim() == "" || str.trim().startsWith("description") || str.trim().startsWith("group-object")) continue;

			if(str.trim().startsWith("object-group")){
				String[] arr = str.trim().split(" ");
				if(arr.length >= 3) {
					parentNode = arr[2];
				} else {
					System.out.println("Invalid record : " + str.trim());
				}
			} else if(str.trim().startsWith("network-object")){
				if(parentNode.trim() == "" || ipAddress.trim() == ""){
					System.out.println("Parent/Object-group/IpAddress not available for this child/network-object : " + str.trim());
				} else {
				    String[] childArr = str.trim().split(" ");
				    if(childArr.length >= 3) {
                        arraylistOfEqRows.add(ipAddress + "," + parentNode + "," + childArr[2]);
                    } else {
                        System.out.println("child/network-object not available for this Parent/Object-group : " + parentNode + ", " + str.trim());
                    }
				}
			} else {
                parentNode = "";
            }
		}

		JavaRDD<String> finalResRDD = javaSparkContext.parallelize(arraylistOfEqRows);
		finalResRDD.saveAsTextFile("C:\\Users\\Dell\\IdeaProjects\\recon\\data\\orders_json\\output1");
	}

}
