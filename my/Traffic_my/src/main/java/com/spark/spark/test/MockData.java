package com.spark.spark.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.bjsxt.spark.util.DateUtils;
import com.bjsxt.spark.util.StringUtils;

/**
 * 模拟数据  数据格式如下：
 * 	卡口ID	monitor_id	车牌号	拍摄时间	车速	通道ID
 * @author Administrator
 *
 */
public class MockData {
	/**
     * date	卡口ID		camera_id	车牌号	拍摄时间	车速	道路ID 区域ID
     * @param sc
     * @param sqlContext
     */
	public static void mock(JavaSparkContext sc, SQLContext sqlContext) {
		List<Row> dataList = new ArrayList<Row>();
		Random random = new Random();
		
		String[] locations = new String[]{"鲁","京","京","京","沪","京","京","深","京","京"}; 
		String date = DateUtils.getTodayDate();
		
		for ( int i = 0; i < 3000; i++ ) {
			String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26)) + StringUtils.fulfuill(5,random.nextInt(99999) + "");
			
			String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");
			for(int j = 0 ; j < random.nextInt(300) ; j++) {
				if(j % 30 == 0 && j != 0){
	       			 baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
	       		}
				String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60)+"") + ":" + StringUtils.fulfuill(random.nextInt(60)+"");
				String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");
				String speed = random.nextInt(260)+"";
				String roadId = random.nextInt(50)+1+"";
				String cameraId = StringUtils.fulfuill(5, random.nextInt(9999)+"");
				String areaId = StringUtils.fulfuill(2,random.nextInt(8)+"");
				Row row = RowFactory.create(date, monitorId, cameraId, car, actionTime, speed, roadId, areaId);
				dataList.add(row);
			}
		}
		
		
		/**
    	 * 2017-4-20 1	22	京A1234 
    	 * 2017-4-20 1	23	京A1234 
    	 * 1 【22,23】
    	 * 1 【22,23,24】
    	 */
		JavaRDD<Row> rowRdd = sc.parallelize(dataList);
		
		StructType cameraFlowSchema = DataTypes.createStructType(Arrays.asList(
					DataTypes.createStructField("date", DataTypes.StringType, true),
					DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
					DataTypes.createStructField("camera_id", DataTypes.StringType, true),
					DataTypes.createStructField("car", DataTypes.StringType, true),
					DataTypes.createStructField("action_time", DataTypes.StringType, true),
					DataTypes.createStructField("speed", DataTypes.StringType, true),
					DataTypes.createStructField("road_id", DataTypes.StringType, true),
	    			DataTypes.createStructField("area_id", DataTypes.StringType, true)
				));
		
		DataFrame df = sqlContext.createDataFrame(rowRdd, cameraFlowSchema);
		
		// 默认在控制台打印出来df里面的20行数据
		df.show();
		df.registerTempTable("monitor_flow_action");
		
//		String sql = 
//				"SELECT * "
//				+ "FROM monitor_flow_action "
//				+ "WHERE "
//				+ "action_time > '2017-02-14 00:00:00'"
//				+ "AND action_time < '2017-02-14 12:05:53'"
//				+ "AND monitor_id IN ('0001','0002')";
//		System.out.println("=======================================");
//    	sqlContext.sql(sql).show();
		
		/**
    	 * monitorAndCameras  key：monitor_id
    	 * 						value:hashSet(camera_id)
    	 */
		Map<String, Set<String>> monitorAndCameras = new HashMap<>();
		
		int index = 0;
		for ( Row row : dataList ) {
			//row.getString(1) monitor_id
			Set<String> sets = monitorAndCameras.get(row.getString(1));
			if ( sets == null ) {
				sets = new HashSet<>();
				monitorAndCameras.put((String)row.getString(1), sets) ;
			}
			
			index++;
			if(index % 1000 == 0){
    			sets.add(StringUtils.fulfuill(5, random.nextInt(99999)+""));
    		} 
			
			//row.getString(2) camera_id
			sets.add(row.getString(2)); 
		}
		
		dataList.clear();
		
		Set<Entry<String, Set<String>>> entrySet = monitorAndCameras.entrySet();
		for ( Entry<String, Set<String>> entry : entrySet ) {
			String monitor_id = entry.getKey();
			Set<String> sets = entry.getValue();
			Row row = null;
			for ( String val : sets ) {
				row = RowFactory.create(monitor_id, val);
				dataList.add(row);
			}
		}
		
		StructType monitorSchema = DataTypes.createStructType(Arrays.asList(
					DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
					DataTypes.createStructField("camera_id", DataTypes.StringType, true)
				));
		
		rowRdd = sc.parallelize(dataList);
		DataFrame monitorDF = sqlContext.createDataFrame(rowRdd, monitorSchema);
		monitorDF.registerTempTable("monitor_camera_info");
		monitorDF.show();
	}
}
