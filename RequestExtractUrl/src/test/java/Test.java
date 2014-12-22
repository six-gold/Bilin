import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {
//        String timeStr = "1418370226";
//        Timestamp ts = new Timestamp(Long.parseLong(timeStr) * 1000);
//        int hour = ts.getHours();
//        if (8<=hour && 22 > hour){
//
//        }
        String[] arr = "ssd sshd".split(" ");
        String url = "http://www.baidu.com/asshd0/0ssd0/ajljf/";
        StringTokenizer st = new StringTokenizer(url, ",");
        while (st.hasMoreElements()) {
            String str = st.nextToken();
            if ("null".equals(str)) {
                break;
            } else {
                System.out.println(str);
            }
        }
        for (String anArr : arr) {

//        String anArr = "SSD";
            String patter = "[^A-Za-z]+" + anArr.toLowerCase() + "[^A-Za-z]*";
//        String patter = "(\\W)ssd(\\W)";
//        Pattern p = Pattern.compile(patter);
//        System.out.println(patter);
//        String patter = "(\\W)*ssd(\\W)*";
            Pattern p = Pattern.compile(patter);
            Matcher m = p.matcher(url);
            while (m.find())
                System.out.println(m.group() + " " + m.start() + " " + m.end());
            
        }
        System.out.println("signin.ebay.com".split("\\.").length);
    }
}
