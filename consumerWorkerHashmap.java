package keti.seolzero.JavaPreprocessing;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;



public class ConsumerWorkerHashmap implements Runnable {
	private Properties prop;
	private String topic;
	private String threadName;
	private KafkaConsumer<String, String> consumer;

	ConsumerWorkerHashmap(Properties prop, String topic, int number) {
		this.prop = prop;
		this.topic = topic;
		this.threadName = "consumer-thread-" + number;
	}

	public void run() {
		final Logger logger = Logger.getLogger(ConsumerWorkerHashmap.class);
		consumer = new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList(topic));

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		HashMap<String, Object> map = new HashMap<String, Object>(100000);//초기 용량(capacity)지정

		try {
			System.out.println("== Pre-processing Module == "+ threadName +" ==");
			long endPoint;
			int message_count = 0;
			long startPoint = System.currentTimeMillis(); 
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				//				if(records.count() == 0) {
				//					endPoint = System.currentTimeMillis();
				//					System.out.println("[threadNum] " + threadName + "\n> startPoint: "+ startPoint + "\n> endPoint: "+ endPoint + "\n>>>>>>>>> endPoint-startPoint: "+(endPoint-startPoint) );
				//				}

				//System.out.println("here: "+records.count());

				for (ConsumerRecord<String, String> record : records) {
					//message_count++;
					//System.out.println(threadName + " >> " + record.value());
					//System.out.println( " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  " + threadName + " >> " + record.offset());



					String recordOfKafka = record.value();
					JSONObject notiObj = new JSONObject(recordOfKafka);
					JSONObject conObject = notiObj.getJSONObject("m2m:sgn").getJSONObject("nev").getJSONObject("rep").getJSONObject("m2m:cin").getJSONObject("con");
					//System.out.println("currenntData: " + currentConData);
					double currentLatitude = conObject.getDouble("latitude");
					double currentLongitude = conObject.getDouble("longitude");
					String currentTime = conObject.getString("time");
					Date currentTimeParse = null;
					try {
						currentTimeParse = format.parse(currentTime);
					} catch (ParseException e) {
						e.printStackTrace();
					}

					/* split device AE, container */
					String sur = notiObj.getJSONObject("m2m:sgn").getString("sur");
					String[] surSplitArray = sur.split("/");
					String AE = surSplitArray[4];        
					String container = surSplitArray[5]; 

					JSONObject PreviousConObject = (JSONObject) map.get("previousData001_"+AE);
					if(PreviousConObject == null) {		
						map.put("previousData001_"+AE, conObject);   
						System.out.println("------------------------put data to map");
					}else {
						double previousLatitude = PreviousConObject.getDouble("latitude");
						double previousLongitude = PreviousConObject.getDouble("longitude");
						String previousTime = PreviousConObject.getString("time");
						Date previousTimeParse = null;
						try {
							previousTimeParse = format.parse(previousTime);
						} catch (ParseException e) {
							e.printStackTrace();
						}


						double preprocessingSpeed = getSpeed(previousLatitude, previousLongitude, previousTimeParse, currentLatitude, currentLongitude, currentTimeParse);
						double preprocessingDistance = getDistance(previousLatitude, previousLongitude, currentLatitude, currentLongitude);
						double preprocessingDirection = getDirection(previousLatitude, previousLongitude, currentLatitude, currentLongitude);		
						//System.out.println("-Speed "+ preprocessingSpeed + "\n-Distance: "+ preprocessingDistance + "\n-Direction: "+ preprocessingDirection);
						logger.info("["+ record.offset() +"] AE: "+ AE 									
								+ "\n    -[previousData]   Latitude: " + previousLatitude + " Longitude: " + previousLongitude + " Time: " + previousTimeParse
								+ "\n    -[currentData]     Latitude: " + currentLatitude + " Longitude: " + currentLongitude + " Time" + currentTimeParse
								+ "\n    -[Preprocessing]  Speed(m/s) "+ preprocessingSpeed + ", Distance(m): "+ preprocessingDistance + ", Direction: "+ preprocessingDirection);

						/*set previousData to HashMap*/               
						map.put("previousData001_"+AE, conObject);

					}

					if(record.offset() == 33332) {
						endPoint = System.currentTimeMillis();
						System.out.println("[threadNum] " + threadName + "\n> startPoint: "+ startPoint + "\n> endPoint: "+ endPoint + "\n>>>>>>>>> endPoint-startPoint: "+(endPoint-startPoint) );
						logger.info("[threadNum] " + threadName + "\n> startPoint: "+ startPoint + "\n> endPoint: "+ endPoint + "\n>>>>>>>>> endPoint-startPoint: "+(endPoint-startPoint) );
					}
					//System.out.println(threadName + " : " + message_count);

					//					if(message_count == 33333) { 
					//						long end = System.currentTimeMillis();
					//						System.out.println( "Pre-processed data count: " + message_count);
					//						System.out.println( "Time taken(ms): " + ( end - startPoint )); 
					//
					//						//message_count++; 
					//					}

				}//for



			}//while

		} catch (WakeupException e) {
			System.out.println(threadName + " trigger WakeupException");
		} finally {
			System.out.println(threadName + " gracefully shutdown");
			//consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}




	/*Speed*/
	public static double getSpeed(double previousLatitude, double previousLongitude, Date previousTimeParse, double currentLatitude, double currentLongitude, Date currentTimeParse) {
		double distancePerM = getDistance(previousLatitude, previousLongitude, currentLatitude, currentLongitude);//이동거리(m)
		long TimeDiff = (currentTimeParse.getTime() - previousTimeParse.getTime())/1000; // 단위:s
		double computevelocity = computespeed(TimeDiff, distancePerM);//이동속도

		return computevelocity;
	}

	/*Distance*/
	public static double getDistance(double previousLatitude, double previousLongitude, double currentLatitude, double currentLongitude) {
		double p = 0.017453292519943295;    // Math.PI / 180
		double a = 0.5 - Math.cos((currentLatitude - previousLatitude) * p)/2 + Math.cos(previousLatitude * p) * Math.cos(currentLatitude * p) * (1 - Math.cos((currentLongitude - previousLongitude) * p))/2;
		return (12742 * Math.asin(Math.sqrt(a))*1000);
	}

	/*bearing*/
	public static double getDirection(double lat1, double lon1, double lat2, double lon2){
		double lat1_rad = convertdecimaldegreestoradians(lat1);
		double lat2_rad = convertdecimaldegreestoradians(lat2);
		double lon_diff_rad = convertdecimaldegreestoradians(lon2-lon1);
		double y = Math.sin(lon_diff_rad) * Math.cos(lat2_rad);
		double x = Math.cos(lat1_rad) * Math.sin(lat2_rad) - Math.sin(lat1_rad) * Math.cos(lat2_rad) * Math.cos(lon_diff_rad);
		return (convertradianstodecimaldegrees(Math.atan2(y,x)) + 360) % 360;
	}    

	public static double computespeed (long timediff, double distancediff){
		double tspeed;
		if(distancediff == 0){
			tspeed = 0;  
		}else{
			tspeed = distancediff / timediff;
		}

		return tspeed;
	} 

	/*decimal degree -> radian*/
	public static double convertdecimaldegreestoradians(double deg){
		return (deg * Math.PI / 180);
	}

	/*decimal radian -> degree*/
	public static double convertradianstodecimaldegrees(double rad){
		return (rad * 180 / Math.PI);
	}		


}
