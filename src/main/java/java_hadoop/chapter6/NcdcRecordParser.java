package java_hadoop.chapter6;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

	private static final int MISSING_TEMPERATURE = 9999;
	
	private String stationId = "";
	private String year = "";
	private int airTemperature = MISSING_TEMPERATURE;
	private String quality = "";
	private String record = "";
	
	public void parse(String record) {
		this.record = record;
		stationId = record.substring(0, 10);
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
	
	public boolean isMissingTemperature() {
		return airTemperature == MISSING_TEMPERATURE;
	}
	
	public String getYear() {
		return year;
	}
	
	public int getYearInt() {
		return Integer.parseInt(year);
	}
	
	public int getAirTemperature() {
		return airTemperature;
	}
	
	public String getQuality() {
		return this.quality;
	}
	
	public String getStationId() {
		return this.stationId;
	}
}
