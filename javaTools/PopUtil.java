package taipei_pop;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

import org.apache.commons.lang3.StringEscapeUtils;

public class PopUtil {
	//地號、母號的合併
	//{123, 2} --> "123-2"
	public String indexCombine(int[] input) {
		String output = input[0] + "-" + input[1];
		return output;
	}
	
	//地號、母號的拆除
	// "123-2" --> {123, 2}
	public int[] indexDecompose(String input) {
		String[] sArray = input.split("-");
		int[] iArray = Arrays.stream(sArray).mapToInt(Integer::parseInt).toArray();
		return iArray;
	}
	
	//段、小段的合併
	//{"仁愛", "三"} --> "仁愛段三小段"
	public String sectionCombine(String section[]) {
		String a = "";
		if(!section[1].equals(""))
			a = section[0] + "段" + section[1] + "小段";
		else 
			a = section[0] + "段";
		return a;
	}
	
	//段、小段的拆除
	// "仁愛段三小段" --> {"仁愛", "三"}
	public String[] sectionDecompose(String section) {
		String[] b = section.split("小段");
		String[] c = b[0].split("段");
		return c;
	}
	//阿拉伯數字轉換國字數字 單一 char
	public String arabicToChinese(String input) {
		HashMap<String, String> numbersMap = new HashMap();
		String arabic = "123456789";
		String ch = "一二三四五六七八九";
		for(int i=0;i<9;i++) {
			if(i<=7)
				numbersMap.put(arabic.substring(i, i+1), ch.substring(i, i+1));
			else if(i==8)
				numbersMap.put(arabic.substring(i), ch.substring(i));
		}
		if(numbersMap.get(input)==null)
			return input;
		else
			return numbersMap.get(input);
	}
	
	//中文字 轉換 utf-8 16進位編碼
	public String stringToUnicode(String pureString) throws Exception {
		byte[] utf8Bytes = pureString.getBytes("UTF8");
		String unicode = new String(utf8Bytes,"UTF8");
		return unicode;
	}
	
	//utf-8 16進位編碼 轉換中文字
	public String unicodeToString(String unicode) {
		String pureString = "";
		pureString = StringEscapeUtils.unescapeJava(unicode);
		return pureString;
	}

	public String addPriceComma(String price) {
		return NumberFormat.getNumberInstance(Locale.US).format(new BigInteger(price));
	}
	
	//
}
