package twitter.buzz.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Testで共通に使用するメソッドを扱うクラス.
 * @author yuya
 *
 */
public final class TestEnv {

    /**
     * 特にインスタンスを生成する予定はないのでprivateでからコンストラクタを用意.
     */
    private TestEnv() { }

    /**
     * ファイルをUTF-8で読み込む関数.
     * @param fileName ファイル名
     * @return ファイルの中身.
     */
    public static String readData(final String fileName) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        StringBuffer sb = new StringBuffer();
        try {
            fis = new FileInputStream(fileName);
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
                isr.close();
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "null";
    }
}
