package taipei_pop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonParser;

public class checkTaipei_pop {

	public static void main(String[] args) {
		//連結 psql	
				try {
					//連線psql 下 sql 指令
					Class.forName("org.postgresql.Driver");
					Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/PG_DB","PG_USER","PG_PWD");
					PreparedStatement stmt = con.prepareStatement("select geo_json from taipei_pop;");
					ResultSet Rs = stmt.executeQuery();
					//gson
					JsonParser parser = new JsonParser();
					
					//統計數據
					int totalCnt = 0;
					int landCnt = 0;
					int buildingCnt = 0;
					int errCnt = 0;
					List<Integer> errPosition = new ArrayList();
					
					String jsonString = "";
					while (Rs.next()) {
						totalCnt++;
						jsonString = Rs.getString("geo_json");
						
						String geo_type = parser.parse(jsonString)
								.getAsJsonObject().get("geometry")
								.getAsJsonObject().get("type")
								.getAsString().toLowerCase();
						boolean check0 = geo_type.equals("point");
						boolean check1 = parser.parse(jsonString)
								.getAsJsonObject().get("properties")
								.getAsJsonObject().has("建物標示");
						boolean check2 = geo_type.equals("polygon") || geo_type.equals("multipolygon");
						boolean check3 = parser.parse(jsonString)
								.getAsJsonObject().get("properties")
								.getAsJsonObject().has("105公告地價");
						if(check0 && check1) {
							//System.out.println("建物");
							buildingCnt++;
						}else if(check2 && check3) {
							//System.out.println("土地");
							landCnt++;
						}else {
							errPosition.add(totalCnt);
						}
					}
					
					System.out.println("Total Count : "+totalCnt);
					System.out.println("Land Count : "+landCnt);
					System.out.println("Building Count : "+buildingCnt);
					System.out.println("Error Count : "+errPosition.size());
					System.out.println("Error Row Number : "+StringUtils.join(",",errPosition));
									
				} catch(Exception ex) {
					System.out.println(ex.getMessage());
				}

	}

}
