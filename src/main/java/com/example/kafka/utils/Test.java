package com.example.kafka.utils;

import java.io.File;
import java.io.IOException;

/**
 * 测试主类
 *
 * @author Felix Li
 * @create 2017-12-19-9:17
 */
public class Test {

    public static void main(String[] args) {
        try {
            //图片文件：此图片是需要被识别的图片 
            File file = new File("d:\\111.png");
            String recognizeText = new OCRHelper().recognizeText(file);
            System.out.print(recognizeText + "\t");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}