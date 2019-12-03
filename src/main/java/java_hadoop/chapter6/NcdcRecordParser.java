package java_hadoop.chapter6;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

	private static final int MISSING_TEMPERATURE = 9999;
	
	private String year;
	private int airTemperature;
	private String quality;
	private String record;
	
	public void parse(String record) {
		this.record = record;
		year = record.substring(15, 19);
		
		// Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
		String airTemperatureString;
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
		} else {
			airTemperatureString = record.substring(87, 92);
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		
		quality = record.substring(92, 93);

	}
	
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public boolean isValidTemperature() {
		return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
	}
	
	public boolean isMalformedTemperature() {
		return this.record.contains(" ");
	}
	
	public String getYear() {
		return year;
	}
	
	public int getAirTemperature() {
		return airTemperature;
	}
	
	public String getStationId() {
		throw new NotImplementedException("method getStationId() is not implemented.");
		//return "1";
	}
}
