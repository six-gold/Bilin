package com.bilin.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Spliter {

    public static ArrayList<String> splits(String str, String split){
		ArrayList<String> spl = new ArrayList<String>();
		int begin = 0;
		int len = str.length();
		int end = str.indexOf(split, begin);
		spl.clear();
		while(begin != len && -1 != end){
			spl.add(str.substring(begin, end));
			begin = end+split.length();
			end = str.indexOf(split, begin);
		}
		spl.add(str.substring(begin, len));
		return spl;
	}
	
	public static String getDomain(String url){
		if(url==null||url.trim().equals("")){
			return "unknown";
		}
		String domain = "";
		Pattern p = Pattern.compile("(?<=//)((\\w)+\\.)+\\w+");
		Matcher matcher = p.matcher(url);  
		if(matcher.find()){
			domain = matcher.group();
			if(domain.startsWith("www."))
				domain = domain.substring(4);
		}
		return domain;
	}

    public static String extractDomainFromUrl(String url) {
        URL myurl;
        try {
            myurl = new URL(url);
            String host = myurl.getHost();
            if (host.startsWith("www.")) {
                return host.substring(host.indexOf("www.") + 4);
            } else
                return host;
        } catch (MalformedURLException e) {
            return "unknown";
        }
    }

}
