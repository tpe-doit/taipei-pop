package taipei_pop;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.postgresql.util.PGobject;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVReader;
import taipei_pop.PopUtil;

public class landLink {

	public static void main(String[] args) throws Exception {
		
		//讀取 csv 的檔案
		String csv_path = "C:\\Users\\felix\\Desktop\\市有閒置空間整合查詢平台\\土地.csv"; 
		CSVReader reader = null;
		HashMap<String, String> keyMap = new HashMap();
		HashMap insertMap = new HashMap();
		int zeroCnt = 0;
		int oneCnt = 0;
		int overCnt = 0;
		try {
			reader = new CSVReader(new FileReader(csv_path));
			String[] nextline;
			int count = 0;
			String targetRowid = "";
			
			while((nextline=reader.readNext()) != null) {
				count++;
				//跳過首行欄位名稱
				if(count == 1)
					continue;
						
				if(nextline != null) {
					
					//System.out.println("地段 : "+nextline[5]);
					//System.out.println("地號母號 : "+nextline[6]);
					//System.out.println("地號子號 : "+nextline[7]);
					//System.out.println("面積 : "+nextline[8]);
					
					keyMap.put("地段", nextline[5]);
					keyMap.put("地號母號", nextline[6]);
					keyMap.put("地號子號", nextline[7]);
					keyMap.put("面積", nextline[8]);
					
					//取得目標rowid
					targetRowid = DBSearch(keyMap);
					
					//準備好要 insert 的資訊
					insertMap = parseCSVData(nextline,targetRowid);
					
					//insert
					doInsert(insertMap,targetRowid);
					System.out.println("count : " + count);
				}
			}
					
		}catch(Exception e) {
			System.out.println(e);
		}finally {
			reader.close();
			//System.out.println("zeroCnt : "+zeroCnt);
			//System.out.println("oneCnt : "+oneCnt);
			//System.out.println("overCnt : "+overCnt);
		}

	}
	
	private static void doInsert(HashMap insertMap, String targetRowid) throws Exception {
		//連結資料庫
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/pg_db","PG_USER","PG_PWD");
		String insertSql = "insert into taipei_pop (block, road, road_no, land_no, ser_no01, ser_no02, area, unit, id, geo_json) values "+
				"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " ;
		PreparedStatement pst = con.prepareStatement(insertSql);
		pst.setString(1, insertMap.get("block").toString());
		pst.setString(2, insertMap.get("road").toString());
		pst.setString(3, insertMap.get("road_no").toString());
		pst.setString(4, insertMap.get("land_no").toString());
		pst.setString(5, insertMap.get("ser_no01").toString());
		pst.setString(6, insertMap.get("ser_no02").toString());
		pst.setDouble(7, Double.parseDouble(insertMap.get("area").toString().trim()));
		pst.setString(8, insertMap.get("unit").toString());
		pst.setString(9, insertMap.get("id").toString());
		pst.setObject(10, (PGobject)insertMap.get("geo_json"));
		pst.execute();
		pst.close();
		con.close();
	}

	private static HashMap parseCSVData(String[] nextline, String targetRowid) throws Exception {
		String block = nextline[4];
		String road = nextline[5];
		String road_no = nextline[6]+ (nextline[7].equals("")? "": "-"+nextline[7]);
		String land_no = nextline[6]+ (nextline[7].equals("")? "": "-"+nextline[7]);
		String ser_no01 = nextline[5];
		String ser_no02 = nextline[6]+ (nextline[7].equals("")? "": "-"+nextline[7]);
		Double area = Double.parseDouble(nextline[8]);
		String unit = "臺北市政府" + nextline[1];
		String id = "1";
		PGobject geo_json = parseJSONData(nextline, targetRowid);
		
		HashMap dataMap = new HashMap();
		dataMap.put("block", block);
		dataMap.put("road", road);
		dataMap.put("road_no", road_no);
		dataMap.put("land_no", land_no);
		dataMap.put("ser_no01", ser_no01);
		dataMap.put("ser_no02", ser_no02);
		dataMap.put("area", area);
		dataMap.put("unit", unit);
		dataMap.put("id", id);
		dataMap.put("geo_json", geo_json);
		
		return dataMap;
	}

	private static PGobject parseJSONData(String[] nextline, String targetRowid) throws Exception {
		//0. 透過 rowid 找到原始的 geo_json 資料
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/PG_DB","PG_USER","PG_PWD");
		String sql = "select geo_json from taipei_pop where rowid = '"+targetRowid+"'";
		PreparedStatement pst = con.prepareStatement(sql);
		ResultSet Rs = pst.executeQuery();
		String jsonString = "";
		while(Rs.next()) {
			jsonString = Rs.getString("geo_json");
		}
		//1. 得到jsonString 後保留需要的資料，寫入修正後的資料。
		JsonParser parser = new JsonParser();
		JsonObject newJson = new JsonObject();
		JsonObject innerJson = new JsonObject();
		JsonObject oldJson = parser.parse(jsonString).getAsJsonObject();
		JsonObject oldInnerJson = new JsonObject();
		String 地號 = "";
		PopUtil pop = new PopUtil();
		
		if(oldJson.has("type"))
			newJson.addProperty("type", oldJson.get("type").getAsString());
		if(oldJson.has("properties")) {
			oldInnerJson = oldJson.get("properties").getAsJsonObject();
			//原封不動保留的內容
			if(oldInnerJson.has("SECLANDID"))
				innerJson.addProperty("SECLANDID", oldInnerJson.get("SECLANDID").getAsString());
			if(oldInnerJson.has("LANDID"))
				innerJson.addProperty("LANDID", oldInnerJson.get("LANDID").getAsString());
			if(oldInnerJson.has("SECTCODE"))
				innerJson.addProperty("SECTCODE", oldInnerJson.get("SECTCODE").getAsString());
			if(oldInnerJson.has("SECTION1"))
				innerJson.addProperty("SECTION1", oldInnerJson.get("SECTION1").getAsString());
			if(oldInnerJson.has("LANDCODE"))
				innerJson.addProperty("LANDCODE", oldInnerJson.get("LANDCODE").getAsString());
			if(oldInnerJson.has("TYPE"))
				innerJson.addProperty("TYPE", oldInnerJson.get("TYPE").getAsString());
			if(oldInnerJson.has("AREA"))
				innerJson.addProperty("AREA", oldInnerJson.get("AREA").getAsString());
			if(oldInnerJson.has("CALAREA"))
				innerJson.addProperty("CALAREA", oldInnerJson.get("CALAREA").getAsString());
			if(oldInnerJson.has("Shape_Leng"))
				innerJson.addProperty("Shape_Leng", oldInnerJson.get("Shape_Leng").getAsString());
			if(oldInnerJson.has("Shape_Area"))
				innerJson.addProperty("Shape_Area", oldInnerJson.get("Shape_Area").getAsString());
			//寫入資料清理後的Data
			//照片檔名
			innerJson.addProperty("Pic_File_Name", "A-"+nextline[0].trim());
			//機關別
			innerJson.addProperty("機關別", nextline[1].trim());
			//用地名稱
			innerJson.addProperty("用地名稱", nextline[2].trim());
			//行政區
			innerJson.addProperty("行政區", nextline[4].trim());
			//地段地號
			地號 = nextline[6].trim() + (nextline[7].trim().equals("") ? "" : "-"+nextline[7].trim());
			innerJson.addProperty("地段地號", nextline[5]+地號+"號");
			//面積
			innerJson.addProperty("面積(m2)", nextline[8].trim());
			//都市計畫使用分區
			innerJson.addProperty("都市計畫使用分區", nextline[9].trim());
			//所有權取得原因 null
			if(!nextline[10].trim().equals(""))
				innerJson.addProperty("所有權取得原因", nextline[10].trim());
			//管理權取得日期  null
			if(!nextline[11].trim().equals(""))
				innerJson.addProperty("管理權取得日期", nextline[11].trim());
			//地上物及使用現況 null
			if(!nextline[12].trim().equals(""))
				innerJson.addProperty("地上物及使用現況", nextline[12].trim());
			//低度利用原因 not null
			innerJson.addProperty("低度利用原因", nextline[13].trim());
			//規劃設計 null
			if(!nextline[14].trim().equals(""))
				innerJson.addProperty("規劃設計", nextline[14].trim());
			//預定開發方式 null
			if(!nextline[15].trim().equals(""))
				innerJson.addProperty("預定開發方式", nextline[15].trim());
			//未來營運方式 null
			if(!nextline[16].trim().equals(""))
				innerJson.addProperty("未來營運方式", nextline[16].trim());
			//列管日期 null
			if(!nextline[17].trim().equals(""))
				innerJson.addProperty("列管日期", nextline[17].trim());
			//長期處理情形 null
			if(!nextline[18].trim().equals(""))
				innerJson.addProperty("長期處理情形", nextline[18].trim());
			//短期處理情形 not null
			if(!nextline[19].trim().equals(""))
				innerJson.addProperty("短期處理情形", nextline[19].trim());
			//105年公告現值 not null
			innerJson.addProperty("105年公告現值", "新台幣 "+pop.addPriceComma(nextline[20].trim())+ " 元");
			//105年公告地價 not null
			innerJson.addProperty("105年公告地價", "新台幣 "+pop.addPriceComma(nextline[21].trim())+ " 元");
			//加完後寫入 newJson
			newJson.add("properties", innerJson);
		}
		if(oldJson.has("geometry"))
			newJson.add("geometry", oldJson.get("geometry").getAsJsonObject());
		
		//PGobject
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(pop.stringToUnicode(newJson.toString()));
		
		return jsonObject;
	}

	private static String DBSearch(HashMap<String, String> keyMap) throws Exception {
		//System.out.println("********* Into DBSearch *********");
		
		//連線psql 下 sql 指令
		String sql = "";
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/PG_DB","PG_USER","PG_PWD");
		PreparedStatement stmt = con.prepareStatement("select geo_json, rowid from taipei_pop;");
		ResultSet Rs = stmt.executeQuery();
		//gson
		JsonParser parser = new JsonParser();
		
		//init data
		String jsonString = "";
		String 段 = "";
		String 小段 = "";
		String 地號 = "";
		String 面積 = "";
		String[] 地段 = {"", ""};
		String 地段String = "";
		String 地號String = "";
		String 面積String = "";
		PopUtil pop = new PopUtil();
		boolean check地段 = false;
		boolean check地號 = false;
		boolean check面積 = false;
		int popIndex = 0;
		int matchCnt = 0;
		String rowid = "";
		//逐筆檢查
		while (Rs.next()) {
			popIndex ++;
			//System.out.println("Index : "+popIndex);
			jsonString = Rs.getString("geo_json");
			
			if(isLand(jsonString)) {
				//取得每筆在 taipei_pop 的資料
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("段")) {
					段 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("段")
							.getAsString();
				}
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("小段")) {
					小段 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("小段")
							.getAsString();					
				}
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("地號")) {
					地號 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("地號")
							.getAsString();					
				}
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("面積(m2)")) {
					面積 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("面積(m2)")
							.getAsString();					
				}
				//資料 parse
				地段[0] = 段.split("段")[0];
				地段[1] = 小段.split("小段")[0];
				地段String = pop.sectionCombine(地段).replace(" ", "");
				地號String = keyMap.get("地號母號");
				if(!keyMap.get("地號子號").equals("")) 
					地號String += "-"+keyMap.get("地號子號");
				
				//資料比對
				//System.out.println(地段String+"     "+keyMap.get("地段"));
				//System.out.println(地號+"     "+地號String);
				
				check地段 = 地段String.equals(keyMap.get("地段"));
				check地號 = 地號.equals(地號String);
				check面積 = areaCheck(面積,keyMap.get("面積"));
				
				if(check地段 && check地號 && check面積) {
					//System.out.println("找到match : "+popIndex);
					matchCnt++;
					rowid = Rs.getString("rowid");
				}
			}
			
		}
		
		stmt.close();
		con.close();
		
		if(matchCnt == 0) {
			//System.out.println(keyMap);
			rowid = "126";
		}
		//System.out.println("rowid : "+rowid);
		
		return rowid;
	}
	
	private static boolean areaCheck(String 面積0, String 面積1) {
		面積0 = 面積0.replace(",", "");
		面積1 = 面積1.replace(",", "");
		//System.out.println(面積0+"     "+面積1);
		double value0 = Double.parseDouble(面積0);
		double value1 = Double.parseDouble(面積1);
		return value0 == value1;
	}

	private static boolean isLand(String jsonString) {
		JsonParser parser = new JsonParser();
		String geo_type = parser.parse(jsonString)
				.getAsJsonObject().get("geometry")
				.getAsJsonObject().get("type")
				.getAsString().toLowerCase();
		boolean check0 = geo_type.equals("point");
		boolean check1 = parser.parse(jsonString)
				.getAsJsonObject().get("properties")
				.getAsJsonObject().has("建築完成日期");
		boolean check2 = geo_type.equals("polygon") || geo_type.equals("multipolygon");
		boolean check3 = parser.parse(jsonString)
				.getAsJsonObject().get("properties")
				.getAsJsonObject().has("105公告地價");
		if(check2 && check3) {
			return true;
		}else {
			return false;
		}
	}

}
