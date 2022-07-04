package java_hadoop.atguigu.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {

	public final static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_TIME_FORMAT);
	
	public static String getDefaultTimeString() {
		Date sysdate = Calendar.getInstance().getTime();
		return sdf.format(sysdate);
	}

	public static long getTimeStamp() {
		return Calendar.getInstance().getTime().getTime();
	}
}
