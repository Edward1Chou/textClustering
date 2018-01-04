package transform_encoding;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
/**
 * 转换中文编码,并添加到一个文件中
 * Created by zcy on 18-1-3.
 */
public class TransformEncodingToOne {
    public static void main(String[] args) {
        String parentStr = "data/文本分类语料库";
        // File dataParent = new File(parentStr);

        List<File> files = getFiles(parentStr);
        String output ="data/data.txt";

        System.out.println(transform(files,output));
    }

    public static List<File> getFiles(String fileStr){
        File file = new File(fileStr);
        List<File> files = new ArrayList<File>();
        if(file.isDirectory()){
            for(File f:file.listFiles()){
                if(f.isDirectory()){
                    files.addAll(getFiles(f.getAbsolutePath()));
                }else{
                    files.add(f);
                }
            }
        }else{
            files.add(file);
        }
        return files;
    }


    public static boolean transform(List<File> files,String output){
        BufferedReader reader = null;
        InputStreamReader isr = null;
        OutputStreamWriter out = null;
        try {
            out = new OutputStreamWriter(new FileOutputStream(new File(output)), "UTF-8");
            for(File file :files) {
                isr = new InputStreamReader(new FileInputStream(file), "GBK");
                reader = new BufferedReader(isr);
                String line = null;
                out.append(file.getName().replaceAll(".TXT",".txt")+"\t");// 后缀改为小写
                // 一次读入一行，直到读入null为文件结束
                while ((line = reader.readLine()) != null) {
                    out.append(line + " ");
                }
                out.write("\n");
                out.flush();
                isr.close();
                reader.close();
            }

            out.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return false;
    }
}

