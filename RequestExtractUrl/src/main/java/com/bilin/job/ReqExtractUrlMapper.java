package com.bilin.job;

import com.bilin.main.Config;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.bilin.utils.DateTransformer;
import com.bilin.utils.Spliter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ReqExtractUrlMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ArrayList<String> str = new ArrayList<String>();
    private Map<String, Integer> lineOfLog = null;
    private ArrayList<String> format = null;
    private MultipleOutputs<Text, Text> multipleOutputs;
    private String swtich;
    private static enum WrongLog {
        WRONGLOG, OUTBOUND, BLACKLIST
    }

    public void loadConfig(Context context) {
        String logType = context.getConfiguration().get("logType");
        String filePath = context.getConfiguration().get("property_file_path");
        Config.getInstance().loadConfig(logType, filePath);
        format = Config.getFormatLog();
        lineOfLog = Config.getLineOfLog();
        swtich = context.getConfiguration().get("switch");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.loadConfig(context);
        multipleOutputs = new MultipleOutputs<Text, Text>(context);

        String line_file = "";
        for (String item : format)
            line_file = line_file.concat(item.concat("\t"));

        multipleOutputs.write(new Text(line_file.substring(0, line_file.length() - 10)), new Text("currency"), "prelytix/req_" + DateTransformer.getDate());
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        str = Spliter.splits(value.toString(), "\t");
        if (str.size() < Config.getLength()) {
            System.out.println(str.size());
            context.getCounter(WrongLog.WRONGLOG).increment(1);
            return;
        }

        //====================================================
        if (Integer.parseInt(swtich) != 1) {
            int rate = Integer.parseInt(context.getConfiguration().get("RATE"));
            int number = new Random().nextInt(100) + 1;

            int length = str.get(lineOfLog.get("url")).length();
            if (number <= rate && length <= 250) {

                ArrayList<String> format_log = new ArrayList<String>();
                format_log.clear();
                for (int i = 0; i < format.size() - 1; i++) {
                    format_log.add("");
                }
                int i = 0;
                for (String item : format) {
                    if (lineOfLog.containsKey(item)) {
                        format_log.set(i, str.get(lineOfLog.get(item)));
                    }
                    i++;
                }

                //time
                String ESTTime = DateTransformer.UTCToEST(str.get(lineOfLog.get("date_time")), str.get(lineOfLog.get("time_zone")));
                format_log.set(format.indexOf("date_time"), ESTTime);
                // geo
                ArrayList<String> geo = Spliter.splits(str.get(lineOfLog.get("geo")), "|");
                if (geo.size() == 4) {
                    for (int j = 0; j < 4; j++) {
                        format_log.set(format.indexOf("user_country") + j, geo.get(j));
                    }
                }
                format_log.set(format.indexOf("ip"), format_log.get(format.indexOf("user_ip")));
                format_log.set(format.indexOf("country"), format_log.get(format.indexOf("user_country")));
                format_log.set(format.indexOf("region"), format_log.get(format.indexOf("user_region")));
//                format_log.set(format.indexOf("time_zone"),"UTC-5");

                // no flash
                String flash = str.get(lineOfLog.get("is_flash_allowed"));
                if (flash.equalsIgnoreCase("true"))
                    format_log.set(format.indexOf("with_flash"), "yes");
                else if (flash.equalsIgnoreCase("false"))
                    format_log.set(format.indexOf("with_flash"), "no");
                else
                    format_log.set(format.indexOf("with_flash"), flash);

                String uuid = str.get(lineOfLog.get("uuid"));
                if (uuid.isEmpty())
                    format_log.set(format.indexOf("with_sync"), "0");
                else
                    format_log.set(format.indexOf("with_sync"), "1");

//			format_log.set(format.indexOf("currency"),"USD");

                String line = "";
                for (String val : format_log) {
                    line = line.concat(val.concat("\t"));
                }
                multipleOutputs.write(new Text(line.substring(0, line.length() - 1)), new Text("USD"),
                        "prelytix/req_" + DateTransformer.getDate());
            } else {
                context.getCounter("NOT_USED_LOG", "req_put_away").increment(1);
            }
        }
        //=============================================================

        if (2 == Integer.parseInt(swtich))
            return;
        String url = str.get(Config.getUrl_Pos());

        if (url.length() > 256 || 0 >= url.length()) {
            context.getCounter(WrongLog.OUTBOUND).increment(1);
            return;
        }
        boolean contains;
        for (String k : Config.config.getKeywordsMap().keySet()) {
            if (null != Config.getBlack_list().get(Integer.parseInt(k.substring(0,k.indexOf("."))))) {
                for (String next_blank_key : Config.getBlack_list().get(Integer.parseInt(k.substring(0,k.indexOf("."))))) {
                    if (url.equals(next_blank_key) || url.contains(next_blank_key)) {
                        context.getCounter(WrongLog.BLACKLIST).increment(1);
                        return;
                    }
                }
            }

            for (String next_key_word : Config.config.getKeywordsMap().get(k)) {
                String[] arr = next_key_word.split(" ");
                contains = false;
                for (String anArr : arr) {
//                    String patter = "[^A-Za-z]+" + anArr.toLowerCase() + "[^A-Za-z]+";
//                    Pattern p = Pattern.compile(patter, Pattern.UNICODE_CASE);
//                    Matcher m = p.matcher(url);
                    if (!url.contains(anArr.toLowerCase())) {
//                    if (!m.find()) {
                        contains = false;
                        break;
                    } else
                        contains = true;
                }
                if (contains) {
                    context.write(new Text(k + "." + url), new Text("1"));
                }
            }
        }

    }

    @Override
    protected void cleanup(
            Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
