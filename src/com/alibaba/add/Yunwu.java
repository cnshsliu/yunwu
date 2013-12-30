package com.alibaba.add;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;

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
			 * Date visit_date = null; try { visit_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(visit_datetime); } catch (ParseException e) { e.printStackTrace(); } Calendar visit_cal = Calendar.getInstance(); visit_cal.setTime(visit_date);
			 */
			userText.set(user_id);
			infoText.set(brand_id + "///" + type + "///" + visit_datetime);
			context.write(userText, infoText);
		}
	}

	public static class YWReducer extends Reducer<Text, Text> {
		private Text result_brand = new Text();
		private Record result = null;
		final static Logger logger = Logger.getLogger(YWReducer.class);

		void addShouldBuy(Connection conn, String brand, int score) throws SQLException {
			PreparedStatement prepps = conn.prepareStatement("INSERT INTO user_brand values (?,8,?,?,0,0,0,?)");
			prepps.setString(1, brand);
			prepps.setTimestamp(2, Timestamp.valueOf("2013-08-01 00:00:00"));
			prepps.setTimestamp(3, Timestamp.valueOf("2013-08-01 00:00:00"));
			prepps.setInt(4, score);
			prepps.execute();
		}

		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.aliyun.odps.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, com.aliyun.odps.mapreduce.ReduceContext)
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			String refDate = "2013-08-01 00:00:00";
			long inOneWeek = 7 * 24 * 60 * 60 * 1000;
			try {
				Class.forName("org.h2.Driver");
				conn = DriverManager.getConnection("jdbc:h2:mem:test");
				String sql = "CREATE TABLE user_brand (BRAND_ID varchar(10), TYPES int, TS timestamp, COMPTS timestamp, AGE_M int, AGE_W int, AGE_D int, score REAL)";
				Statement stat = conn.createStatement();
				stat.execute(sql);
				PreparedStatement prep_new = conn.prepareStatement("INSERT INTO user_brand values (?,?,?,?,0,0,0,0.0)");

				int inserted_count = 0;
				for (Text val : values) {
					String infoString = val.toString();
					StringTokenizer st = new StringTokenizer(infoString, "///");
					prep_new.setString(1, st.nextToken());
					prep_new.setInt(2, Integer.valueOf(st.nextToken()).intValue());
					prep_new.setTimestamp(3, Timestamp.valueOf(st.nextToken()));
					prep_new.setTimestamp(4, Timestamp.valueOf("2013-08-01 00:00:00"));
					prep_new.execute();
					inserted_count++;

				}
				logger.info(key.toString() + "\t" + inserted_count + " records inserted");

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

				String sqlQ = "update user_brand set AGE_M=MONTH(COMPTS)-MONTH(TS), AGE_W = WEEK(COMPTS)- WEEK(TS)+1, AGE_D = DAY_OF_YEAR(COMPTS)-DAY_OF_YEAR(TS)";
				stat.execute(sqlQ);

				// 先取所有商品
				String tmp0 = "select DISTINCT BRAND_ID from user_brand";
				ResultSet allBrandsRS = stat.executeQuery(tmp0);
				Vector<String> allBrands = new Vector<String>();
				while (allBrandsRS.next()) {
					allBrands.add(allBrandsRS.getString("BRAND_ID"));
				}
				for (Iterator itr = allBrands.iterator(); itr.hasNext();) {
					String brand = (String) itr.next();
					// 过去的购买记录，过去四个月，至少有两个月，每月都有至少一次购买，则后月会买
					String tmpm1 = "select * from user_brand where BRAND_ID=? and TYPES=0 and AGE_M=?";
					PreparedStatement ps = conn.prepareStatement(tmpm1);
					int count = 0;
					for (int i = 0; i < 4; i++) {
						ps.setString(1, brand);
						ps.setInt(2, i + 1);
						ResultSet tmprs = ps.executeQuery();
						if (tmprs.next()) {
							count++;
						}
					}
					if (count >= 2) {
						addShouldBuy(conn, brand, 100);
					}

					// 过去的购买记录，过去四个周，至少有两个周有至少一次购买记录，则后月会买
					String tmpw1 = "select * from user_brand where BRAND_ID=? and TYPES=0 and AGE_W=?";
					ps = conn.prepareStatement(tmpw1);
					count = 0;
					for (int i = 0; i < 4; i++) {
						ps.setString(1, brand);
						ps.setInt(2, i + 1);
						ResultSet tmprs = ps.executeQuery();
						if (tmprs.next()) {
							count++;
						}
					}
					if (count >= 2) {
						addShouldBuy(conn, brand, 100);
					}

					// 过去的购买记录，过去7天中，至少有三天至少一次购买记录，则后月会买
					String tmpd1 = "select * from user_brand where BRAND_ID=? and TYPES=0 and AGE_D=?";
					ps = conn.prepareStatement(tmpd1);
					count = 0;
					for (int i = 0; i < 7; i++) {
						ps.setString(1, brand);
						ps.setInt(2, i + 1);
						ResultSet tmprs = ps.executeQuery();
						if (tmprs.next()) {
							count++;
						}
					}
					if (count >= 3) {
						addShouldBuy(conn, brand, 100);
					}

					// 对同一品牌，如果加了购物车，但后面没有购买记录，则在8月一定会购买
					// 先找到品牌的最后一次加购物车时间
					String tmps1 = "select * from user_brand where BRAND_ID=? and TYPES=3 ORDER BY TS DESC LIMIT 1";
					ps = conn.prepareStatement(tmps1);
					ps.setString(1, brand);
					ResultSet tmprs = ps.executeQuery();
					if (tmprs.next()) {
						Timestamp ts = tmprs.getTimestamp("TS");
						int age_m = tmprs.getInt("AGE_M");
						String tmps2 = "select * from user_brand where BRAND_ID=? AND TYPES=0 AND TS>=?";
						ps = conn.prepareStatement(tmps2);
						ps.setString(1, brand);
						ps.setTimestamp(2, ts);
						ResultSet tmprs2 = ps.executeQuery();
						if (tmprs2.next() == false) {
							addShouldBuy(conn, brand, 60 / age_m);
						}
					}

					tmps1 = "select * from user_brand where BRAND_ID=? and (TYPES=2 OR TYPES=4) ORDER BY TS DESC LIMIT 1";
					ps = conn.prepareStatement(tmps1);
					ps.setString(1, brand);
					tmprs = ps.executeQuery();
					boolean find_24 = false;
					if (tmprs.next()) {
						find_24 = true;
						Timestamp ts = tmprs.getTimestamp("TS");
						int age_w = tmprs.getInt("AGE_W");
						String tmps2 = "select * from user_brand where BRAND_ID=? AND TYPES=0 AND TS>=?";
						ps = conn.prepareStatement(tmps2);
						ps.setString(1, brand);
						ps.setTimestamp(2, ts);
						ResultSet tmprs2 = ps.executeQuery();
						if (tmprs2.next() == false) {
							addShouldBuy(conn, brand, 30 / age_w);
						}
					}

					// 点击，点击记分。
					float factor = (float) (find_24 ? 1.2 : 1.0);
					float baseScore = (float) ((60.0 / 3.0) * factor);
					sqlQ = "update user_brand set score=score + " + baseScore + "/AGE_D where TYPES=1 and BRAND_ID='" + brand + "'";
					stat.execute(sqlQ);

				}

				String sqlA = null;
				ResultSet rs = null;
				HashMap hm = new HashMap();
				/*
				 * sqlA="select * from user_brand ORDER BY TS"; // String sqlA = // "select * from  (select BRAND_ID, count(*) as C from user_brand where TYPES= 1 group by brand_id) where C>=3";
				 * 
				 * 
				 * rs = stat.executeQuery(sqlA); while (rs.next()) { System.out.println(rs.getString("BRAND_ID") + "\t" + rs.getInt("TYPES") + "\t" + rs.getTimestamp("TS") + "\t" + rs.getInt("AGE_M") + "\t" + rs.getInt("AGE_W") + "\t" + rs.getInt("AGE_D") + "\t" + rs.getFloat("SCORE")); }
				 */
				sqlA = "select SUM(score) as tscore, BRAND_ID from user_brand group by BRAND_ID";
				rs = stat.executeQuery(sqlA);
				while (rs.next()) {
					float tscore = rs.getFloat("TSCORE");
					String brand = rs.getString("BRAND_ID");
					if (tscore >= 60)
						hm.put(brand, "V");
				}

				//

				/*
				 * result_brand.set(rs.getString(1)); result.set(0, key); result.set(1, result_brand); context.write(result);
				 */
				int brand_count = 0;
				StringBuilder sb = new StringBuilder();
				for (Iterator it = hm.keySet().iterator(); it.hasNext();) {
					String aBrand = (String) it.next();
					if (brand_count > 0) {
						sb.append(",");
					}
					sb.append(aBrand);
					brand_count++;
				}
				if (brand_count > 0) {
					result_brand.set(sb.toString());
					result.set(0, key);
					result.set(1, result_brand);
					context.write(result);
				}

			} catch (Exception ex) {
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
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in_table> <out_table>");
			System.exit(2);
		}
		JobConf job = new JobConf();
		job.setMapperClass(YWMapper.class);
		job.setReducerClass(YWReducer.class);
		job.setNumReduceTasks(9999);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableInputFormat.addInput(new TableInfo(args[0]), job);
		TableOutputFormat.addOutput(new TableInfo(args[1]), job);
		JobClient.runJob(job);

	}
}
