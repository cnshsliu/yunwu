package com.alibaba.add;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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

public class Rehuo {
	static int BUY = 0;
	static int CLICK = 1;
	static int BOOKMARK = 2;
	static int CART = 3;
	static int FOLLOW = 4;

	public static class RHMapper extends Mapper<Text, Text> {
		static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private Text userText = new Text();
		private Text infoText = new Text();

		public void map(LongWritable recordNum, Record record, MapContext<Text, Text> context) throws IOException, InterruptedException {
			Calendar dt = Calendar.getInstance();
			String user_id = record.get(0).toString();
			String brand_id = record.get(1).toString();
			String type = record.get(2).toString();
			// int type = Integer.parseInt(record.get(2).toString());
			String visit_datetime = record.get(3).toString();
			/*
			 * Date visit_date = null; try { visit_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(visit_datetime); } catch (ParseException e) { e.printStackTrace(); } Calendar visit_cal = Calendar.getInstance(); visit_cal.setTime(visit_date);
			 */
			userText.set(user_id);
			infoText.set(brand_id + "$" + type + "$" + visit_datetime);
			try {
				dt.setTime(sdf.parse(visit_datetime));
				if (dt.get(Calendar.MONTH) >= 3 && dt.get(Calendar.MONTH) <= 6)
					context.write(userText, infoText);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Action {
		static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String brand = null;
		int types = -1;
		long ms = 0L;
		int age_m = -1;
		int age_w = -1;
		int age_d = -1;

		public Action(String brandS, String typesS, String datetimeS) throws ParseException {
			Calendar baseT = Calendar.getInstance();
			Calendar actionT = Calendar.getInstance();
			baseT.setTime(sdf.parse("2013-08-01 00:00:00"));
			brand = brandS;
			types = Integer.valueOf(typesS).intValue();
			actionT.setTime(sdf.parse(datetimeS));
			ms = actionT.getTimeInMillis();
			int tt = 0;
			tt = baseT.get(Calendar.MONTH);
			tt = actionT.get(Calendar.MONTH);
			tt = baseT.get(Calendar.WEEK_OF_YEAR);
			tt = actionT.get(Calendar.WEEK_OF_YEAR);
			tt = baseT.get(Calendar.DAY_OF_YEAR);
			tt = actionT.get(Calendar.DAY_OF_YEAR);
			age_m = baseT.get(Calendar.MONTH) - actionT.get(Calendar.MONTH);
			age_w = baseT.get(Calendar.WEEK_OF_YEAR) - actionT.get(Calendar.WEEK_OF_YEAR);
			age_d = baseT.get(Calendar.DAY_OF_YEAR) - actionT.get(Calendar.DAY_OF_YEAR);

		}
	}

	public static class RHReducer extends Reducer<Text, Text> {
		private Record result = null;
		final static Logger logger = Logger.getLogger(RHReducer.class);

		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		int sumArr(int[] arr) {
			int ret = 0;
			for (int i = 0; i < arr.length; i++)
				ret += arr[i];
			return ret;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.alimr_in mr_outyun.odps.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, com.aliyun.odps.mapreduce.ReduceContext)
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, ReduceContext<Text, Text> context) throws IOException, InterruptedException {

			Set<String> brands = new HashSet<String>();
			Map<String, Vector<Action>> brandActions = new HashMap<String, Vector<Action>>();
			try {
				for (Text val : values) {
					String infoString = val.toString();
					StringTokenizer st = new StringTokenizer(infoString, "$");
					String brandS = st.nextToken();
					brands.add(brandS);
					Action act = new Action(brandS, st.nextToken(), st.nextToken());
					if (brandActions.containsKey(brandS)) {
						brandActions.get(brandS).add(act);
					} else {
						Vector<Action> v = new Vector<Action>();
						v.add(act);
						brandActions.put(brandS, v);
					}
				}

				int brand_selected_count = 0;
				StringBuilder brand_selected_sb = new StringBuilder();
				for (Iterator<String> itr = brands.iterator(); itr.hasNext();) {
					String brand = (String) itr.next();

					int[] month_mark = { 0, 0, 0, 0 };
					int[] week_mark = { 0, 0, 0, 0 };
					int[] day_mark = { 0, 0, 0, 0, 0, 0, 0 };
					long lastCartMs = -1L;
					long lastBuyMs = -1L;
					long lastInterestMs = -1L;
					int lastInterestAgeW = 1;
					int lastCartAgeM = 1;
					float clickWeight = (float) 1.0;
					float score = (float) 0.0;

					for (Iterator<Action> it = brandActions.get(brand).iterator(); it.hasNext();) {
						Action act = (Action) it.next();
						if (act.types == BUY) {
							int idx = act.age_m - 1;
							idx = (idx < 0) ? 0 : idx;
							idx = (idx > 3) ? 3 : idx;
							month_mark[idx] = 1;

							if (act.age_w < 4) {
								week_mark[act.age_w] = 1;
							}

							if (act.age_d - 1 < 7) {
								day_mark[act.age_d - 1] = 1;
							}
							lastBuyMs = act.ms > lastBuyMs ? act.ms : lastBuyMs;
							clickWeight = (float) 1.5;
						} else if (act.types == CART) {
							if (act.ms > lastCartMs) {
								lastCartMs = act.ms;
								lastCartAgeM = act.age_m;
							}
							clickWeight = (float) 1.5;
						} else if (act.types == FOLLOW || act.types == BOOKMARK) {
							if (act.ms > lastInterestMs) {
								lastInterestMs = act.ms;
								lastInterestAgeW = act.age_w;
							}
							clickWeight = (float) 1.5;
						} else if (act.types == CLICK) {
							score += (float) ((60.0 / 5.0) * clickWeight) / (act.age_d > 0 ? act.age_d : 1);
						}
					}

					if (sumArr(month_mark) >= 2 || sumArr(week_mark) >= 2 || sumArr(day_mark) >= 3) {
						score += 60.0;
					}
					if (lastCartMs > -1L && lastBuyMs < lastCartMs) {
						score += 60.0 / lastCartAgeM;
					}
					if (lastInterestMs > -1L && lastBuyMs < lastInterestMs) {
						score += 30.0 / (lastInterestAgeW + 1);
					}

					// score += 61;

					if (score >= 60) {
						if (brand_selected_count > 0) {
							brand_selected_sb.append(",");
						}
						brand_selected_sb.append(brand);
						brand_selected_count++;
					}
				}

				if (brand_selected_count > 0) {
					Text result_brand = new Text();
					String resText = brand_selected_sb.toString();
					if (resText.length() > 100)
						System.err.println("Warning, result too long");
					result_brand.set(resText);
					result.set(0, key);
					result.set(1, result_brand);
					context.write(result);
				}

			} catch (Exception ex) {
				logger.error(ex.getLocalizedMessage());
			} finally {
				brandActions = null;
				brands = null;
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
		job.setMapperClass(RHMapper.class);
		job.setReducerClass(RHReducer.class);
		job.setNumReduceTasks(100);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableInputFormat.addInput(new TableInfo(args[0]), job);
		TableOutputFormat.addOutput(new TableInfo(args[1]), job);
		JobClient.runJob(job);

	}
}
