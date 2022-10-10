package com.ruth.stormDemo.util;

import java.io.*;

/**
 * @Titile :  getFileContent
 * @Author :  ruthlessHardt
 */
public class FileUtil {
    public static void getFileContent(Object fileInPath) throws IOException {
        BufferedReader br = null;
        if (fileInPath == null) {
            return;
        }
        if (fileInPath instanceof String) {
            //如果是字符串路径
            br = new BufferedReader(new FileReader(new File((String) fileInPath)));
        } else if (fileInPath instanceof InputStream) {
            //如果是输入流
            br = new BufferedReader(new InputStreamReader((InputStream) fileInPath));
        }
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
    }
}