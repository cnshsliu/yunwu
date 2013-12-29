package com.alibaba.add;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.alibaba.add.WordCount.WCReducer;
import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class Yunwu {
	static int BUY = 0;
	static int CLICK = 1;
	static int BOOKMARK = 2;
	static int CART = 3;
	static int FOLLOW = 4;

	static int WEIGHT_BUY = 1;
	static int WEIGHT_CLICK = 1;
	static int WEIGHT_BOOKMARK = 10;
	static int WEIGHT_CART = 5;
	static int WEIGHT_FOLLOW = 4;

	static Connection conn = null;

	public static class YWMapper extends Mapper<Text, Text> {
		private Text userText = new Text();
		private Text infoText = new Text();

		public void map(LongWritable recordNum, Record record, MapContext<Text, Text> context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(record.get(0).toString());
			String user_id = record.get(0).toString();
			String brand_id = record.get(1).toString();
			String type = record.get(2).toString();
			// int type = Integer.parseInt(record.get(2).toString());
			String visit_datetime = record.get(3).toString();
			/*
			 * Date visit_date = null; try { visit_date = new
			 * SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
			 * Locale.ENGLISH).parse(visit_datetime); } catch (ParseException e)
			 * { e.printStackTrace(); } Calendar visit_cal =
			 * Calendar.getInstance(); visit_cal.setTime(visit_date);
			 */
			userText.set(user_id);
			infoText.set(brand_id + "///" + type + "///" + visit_datetime);
			System.out.println(infoText);
			context.write(userText, infoText);
		}
	}

	public static class YWReducer extends Reducer<Text, Text> {
		private Text result_brand = new Text();
		private Record result = null;
		final static Logger logger = Logger.getLogger(WCReducer.class);

		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.aliyun.odps.mapreduce.Reducer#reduce(java.lang.Object,
		 * java.lang.Iterable, com.aliyun.odps.mapreduce.ReduceContext)
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			System.out.println("Key=" + key);
			String refDate = "2013-08-01 00:00:00";
			long inOneWeek = 7 * 24 * 60 * 60 * 1000;
			try {
				Class.forName("org.h2.Driver");
				conn = DriverManager.getConnection("jdbc:h2:mem:test");
				String sql = "CREATE TABLE user_brand (BRAND_ID varchar(10), TYPES int, TS timestamp, AGE_M int, AGE_W int, AGE_D int, score REAL)";
				Statement stat = conn.createStatement();
				stat.execute(sql);
				PreparedStatement prep = conn.prepareStatement("INSERT INTO user_brand values (?,?,?,0,0,0,0.0)");

				int inserted_count = 0;
				for (Text val : values) {
					String infoString = val.toString();
					StringTokenizer st = new StringTokenizer(infoString, "///");
					prep.setString(1, st.nextToken());
					prep.setInt(2, Integer.valueOf(st.nextToken()).intValue());
					prep.setTimestamp(3, Timestamp.valueOf(st.nextToken()));
					prep.execute();
					inserted_count++;

				}
				System.out.println(inserted_count + " records inserted");

				// 开始预测 对行为评分，设定阀值，超过阀值的计做将要购买 评分规则如下：
				// 1. 两种计分：行为分 和 时间分
				// 2. 对同一品牌的多次行为，重复记分
				// 2. 行为分：
				// 2.1 加购物车后没有购买，计10分，并乘 购物车时间因子
				// 2.2 加购物车后已购买，计1分，并乘已购买时间因子
				// 2.3 关注后没有购买，计9分， 并乘关注时间因子，已购买，计1分，并乘已购买时间因子；
				// 2.4 收藏后没有购买，计9分，并乘收藏时间因子，已购买，计1分，并乘已购买时间因子；
				// 2.5 每次点击计2分，并乘时间因子
				// 3. 时间因子计算方法
				// 3.1 购物车时间因子：(1-（8.1-TS)/(8.1-3.31))*10
				//

				String sqlQ = "update user_brand set AGE_M=8-MONTH(TS), AGE_W = WEEK(PARSEDATETIME('2013-08-01 00:00:00', 'yyyy-MM-dd HH:mm:ss'))- WEEK(TS), AGE_D = DAY_OF_YEAR(PARSEDATETIME('2013-08-01 00:00:00', 'yyyy-MM-dd HH:mm:ss'))-DAY_OF_YEAR(TS)";
				stat.execute(sqlQ);

				sqlQ = "update user_brand set score=score + 60/(3*AGE_D) where TYPES=1";
				stat.execute(sqlQ);
				// 在过去一周内点击某件商品的次数超过3次，会购买该产品
				String sqlA = "select * from user_brand where score >=0.0 ";
				// String sqlA =
				// "select * from  (select BRAND_ID, count(*) as C from user_brand where TYPES= 1 group by brand_id) where C>=3";

				HashMap hm = new HashMap();

				ResultSet rs = stat.executeQuery(sqlA);
				while (rs.next()) {
					System.out.println(rs.getString("BRAND_ID") + "\t" + rs.getInt("TYPES") + "\t" + rs.getInt("AGE_M") + "\t" + rs.getInt("AGE_W") + "\t" + rs.getInt("AGE_D") + "\t" + rs.getFloat("SCORE"));
					hm.put(rs.getString(1), "V");
				}

				//

				/*
				 * result_brand.set(rs.getString(1)); result.set(0, key);
				 * result.set(1, result_brand); context.write(result);
				 */
				for (Iterator it = hm.keySet().iterator(); it.hasNext();) {
					result_brand.set((String) it.next());
					result.set(0, key);
					result.set(1, result_brand);
					context.write(result);
					System.out.println("context.write " + key + ":" + result_brand);
				}

			} catch (Exception ex) {
				System.err.println(ex.getLocalizedMessage());
				logger.error(ex.getLocalizedMessage());
			} finally {
				try {
					conn.close();
				} catch (Exception connex) {
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String infoString = "ab///cd///efg";
		StringTokenizer st = new StringTokenizer(infoString, "///");
		while (st.hasMoreTokens()) {
			System.out.println("[" + st.nextToken() + "]");
		}

		if (args.length != 2) {
			System.err.println("Usage: wordcount <in_table> <out_table>");
			System.exit(2);
		}
		JobConf job = new JobConf();
		job.setMapperClass(YWMapper.class);
		// job.setCombinerClass(YWCombiner.class);
		job.setReducerClass(YWReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableInputFormat.addInput(new TableInfo(args[0]), job);
		TableOutputFormat.addOutput(new TableInfo(args[1]), job);
		JobClient.runJob(job);

	}
}
