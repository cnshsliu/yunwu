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
	static int CART_LIVING_PERIOD = 60;
	static int INTEREST_LIVING_PERIOD = 60;
	static float CART_TOP_SCORE = 60.0f;
	static float INTEREST_TOP_SCORE = 40.0f;
	static float MUST_BUY_SCORE = 60.0f;
	static float CLICK_TOP_SCORE = 12.0f;
	static float NORMAL_CLICK_WEIGHT = 1.0f;
	static int DAY_CLICK_CHECK_PERIOD = 7;
	static int DAY_CLICK_CHECK_THRESHOLD = 3;

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
			baseT.setTime(sdf.parse("2013-07-03 00:00:00"));
			brand = brandS;
			types = Integer.valueOf(typesS).intValue();
			actionT.setTime(sdf.parse(datetimeS));
			ms = actionT.getTimeInMillis();
			/*
			 * int tt = 0; tt = baseT.get(Calendar.MONTH); tt = actionT.get(Calendar.MONTH); tt = baseT.get(Calendar.WEEK_OF_YEAR); tt = actionT.get(Calendar.WEEK_OF_YEAR); tt = baseT.get(Calendar.DAY_OF_YEAR); tt = actionT.get(Calendar.DAY_OF_YEAR);
			 */
			age_m = baseT.get(Calendar.MONTH) - actionT.get(Calendar.MONTH);
			age_w = baseT.get(Calendar.WEEK_OF_YEAR) - actionT.get(Calendar.WEEK_OF_YEAR);
			age_d = baseT.get(Calendar.DAY_OF_YEAR) - actionT.get(Calendar.DAY_OF_YEAR) - 1;

		}
	}

	public static class RHReducer extends Reducer<Text, Text> {
		private Record result = null;
		final static Logger logger = Logger.getLogger(RHReducer.class);

		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		int sumArr(int[] arr, int a, int b) {
			int ret = 0;
			if (b > arr.length)
				b = arr.length;
			for (int i = 0; i < b; i++) {
				ret += arr[a + i];
			}
			return ret;
		}

		int briefSumArr(int[] arr, int a, int b) {
			int ret = 0;
			if (b > arr.length)
				b = arr.length;
			for (int i = 0; i < b; i++) {
				if (arr[a + i] > 0)
					ret += 1;
			}
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
				boolean everBuySomething = false;
				boolean everInterestSomething = false;
				for (Iterator<String> itr = brands.iterator(); itr.hasNext();) {
					String brand = (String) itr.next();

					int[] month_buy_mark = { 0, 0, 0, 0, 0 };
					int[] week_buy_mark = { 0, 0, 0, 0, 0 };
					int[] day_buy_mark = { 0, 0, 0, 0, 0, 0, 0 };
					int[] month_click_mark = { 0, 0, 0, 0, 0 };
					int[] week_click_mark = { 0, 0, 0, 0, 0 };
					int[] day_click_mark = { 0, 0, 0, 0, 0, 0, 0 };
					long lastCartMs = -1L;
					long lastBuyMs = -1L;
					long lastInterestMs = -1L;
					int lastInterestAgeW = 1;
					int lastInterestAgeD = -1;
					int lastCartAgeM = 1;
					int lastCartAgeD = -1;
					float clickWeight = NORMAL_CLICK_WEIGHT;
					float score = (float) 0.0;
					float clickScore = (float) 0.0;

					for (Iterator<Action> it = brandActions.get(brand).iterator(); it.hasNext();) {
						Action act = (Action) it.next();
						if (act.types == BUY) {
							month_buy_mark[act.age_m]++;

							if (act.age_w < 5) {
								week_buy_mark[act.age_w]++;
							}

							if (act.age_d < 7) {
								day_buy_mark[act.age_d]++;
							}
							lastBuyMs = act.ms > lastBuyMs ? act.ms : lastBuyMs;
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everBuySomething = true;
						} else if (act.types == CART) {
							if (act.ms > lastCartMs) {
								lastCartMs = act.ms;
								lastCartAgeM = act.age_m;
								lastCartAgeD = act.age_d;
							}
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everInterestSomething = true;
						} else if (act.types == FOLLOW || act.types == BOOKMARK) {
							if (act.ms > lastInterestMs) {
								lastInterestMs = act.ms;
								lastInterestAgeW = act.age_w;
								lastInterestAgeD = act.age_d;
							}
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everInterestSomething = true;
						} else if (act.types == CLICK) {
							month_click_mark[act.age_m]++;

							if (act.age_w < 5) {
								week_click_mark[act.age_w]++;
							}

							if (act.age_d < 7) {
								day_click_mark[act.age_d]++;
							}

							clickScore += (float) (CLICK_TOP_SCORE * clickWeight) / (act.age_d + 1);
						}
					}

					if (briefSumArr(month_buy_mark, 1, 2) == 2) {
						if (month_buy_mark[0] < Math.min(month_buy_mark[1], month_buy_mark[2]))
							score += MUST_BUY_SCORE;
					}

					if (briefSumArr(week_buy_mark, 1, 2) >= 2) {
						if (week_buy_mark[0] < Math.min(week_buy_mark[1], week_buy_mark[2]))
							score += MUST_BUY_SCORE;
					}
					if (briefSumArr(day_buy_mark, 0, 3) >= 3) {
						score += MUST_BUY_SCORE;
					}
					if (lastCartMs > -1L) {
						int tmpweek = lastCartAgeD / 7;
						if (lastCartAgeD <= CART_LIVING_PERIOD) {
							if (lastBuyMs < lastCartMs) {
								score += CART_TOP_SCORE / (tmpweek + 1);
							} else if (month_buy_mark[0] < month_buy_mark[1]) {
								score += CART_TOP_SCORE / (tmpweek + 1);
							}
						}
					}
					if (lastInterestMs > -1L) {
						int tmpweek = lastInterestAgeD / 7;
						if (lastInterestAgeD <= INTEREST_LIVING_PERIOD) {
							if (lastBuyMs < lastInterestMs) {
								score += INTEREST_TOP_SCORE / (tmpweek + 1);
							} else if (month_buy_mark[0] < month_buy_mark[1]) {
								score += INTEREST_TOP_SCORE / (tmpweek + 1);
							}
						}
					}

					for (int i = 0; i < DAY_CLICK_CHECK_PERIOD; i++) {
						if (day_click_mark[i] >= DAY_CLICK_CHECK_THRESHOLD + i) {
							boolean alreadyBought = false;
							for (int j = 0; j <= i; j++) {
								if (day_buy_mark[j] >= 1) {
									alreadyBought = true;
									break;
								}
							}
							if (!alreadyBought) {
								score += MUST_BUY_SCORE / (i + 1);
								if (score >= MUST_BUY_SCORE) {
									break;
								}
							}
						}
					}

					score += clickScore;

					if (score >= MUST_BUY_SCORE) {
						if (brand_selected_count > 0) {
							brand_selected_sb.append(",");
						}
						brand_selected_sb.append(brand);
						brand_selected_count++;
					}
				}

				if (brand_selected_count > 0 && (everBuySomething || everInterestSomething)) {
					Text result_brand = new Text();
					result_brand.set(brand_selected_sb.toString());
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
