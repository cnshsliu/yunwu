package com.alibaba.add;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableComparator;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class WordCount {
	static Connection conn = null;

	public static class WCComparator extends WritableComparator {
		public WCComparator() {
			super(LongWritable.class, true);
		}

		@Override
		public int compare(WritableComparable r1, WritableComparable r2) {
			String k1 = r1.toString();
			String k2 = r2.toString();
			long len1 = k1.length();
			long len2 = k2.length();
			return len1 == len2 ? ((k1.equals(k2)) ? 0 : 1) : (len1 < len2 ? 1 : -1);
		}
	}

	public static class WCMapper extends Mapper<Text, LongWritable> {
		LongWritable one = new LongWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable recordNum, Record record, MapContext<Text, LongWritable> context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			sb.append(record.get(0).toString());
			sb.append(" ");
			sb.append(record.get(1).toString());
			StringTokenizer st = new StringTokenizer(sb.toString());
			while (st.hasMoreElements()) {// trim the ending "."
				String s = st.nextElement().toString().replace(".", "");
				word.set(s);
				context.write(word, one);
			}
		}
	}

	public static class WCReducer extends Reducer<Text, LongWritable> {
		private LongWritable sum = new LongWritable();
		private Record result = null;
		final static Logger logger = Logger.getLogger(WCReducer.class);

		@Override
		protected void setup(ReduceContext<Text, LongWritable> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, ReduceContext<Text, LongWritable> context) throws IOException, InterruptedException {
			int count = 0;

			for (LongWritable val : values) {
				count += val.get();
			}
			sum.set(count);
			result.set(0, key);
			result.set(1, sum);
			context.write(result);
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

		String refDate = "2013-08-01 00:00:00";

		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:test");
		String sql = "CREATE TABLE user_brand (BRAND_ID varchar(10), TYPES int, TS timestamp)";
		Statement stat = conn.createStatement();
		stat.execute(sql);
		sql = "INSERT INTO user_brand VALUES ('618012', 0, '2013-07-24 14:00:00')";
		stat.execute(sql);
		long inOneWeek = 7 * 24 * 60 * 60 * 1000;
		sql = "SELECT TIMESTAMPDIFF('MS', TS, PARSEDATETIME('2013-08-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')) from user_brand";
		ResultSet rs = stat.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getLong(1));
		}
		sql = "SELECT * from user_brand where TIMESTAMPDIFF('MS', TS, PARSEDATETIME('" + refDate + "', 'yyyy-MM-dd HH:mm:ss')) < " + inOneWeek;
		rs = stat.executeQuery(sql);
		while (rs.next()) {
			System.out.println(rs.getString(1) + "   " + rs.getString(2) + "   " + rs.getTimestamp(3));
		}
		/*
		 * JobConf job = new JobConf(); job.setMapperClass(WCMapper.class); job.setReducerClass(WCReducer.class); job.setMapOutputKeyClass(Text.class); job.setMapOutputValueClass(LongWritable.class); // job.setOutputKeyComparatorClass(WCComparator.class); TableInputFormat.addInput(new TableInfo(args[0]), job); TableOutputFormat.addOutput(new TableInfo(args[1]), job); JobClient.runJob(job);
		 */

		conn.close();

	}

}
