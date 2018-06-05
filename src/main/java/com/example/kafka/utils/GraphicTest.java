package com.example.kafka.utils;

import com.by_syk.graphiccr.core.GraphicCTranslator;

import java.io.File;

public class GraphicTest {
    public static void main(String[] args) {
        File testFile1 = new File("D:\\16.png");
        String result1 = GraphicCTranslator.translate(testFile1, GraphicCTranslator.TYPE_1);
        System.out.println(result1);


//        File testFile2 = new File("E:/JavaProjects/GraphicCR/reserve/GraphicC/2/test/2rxl.gif");
//        String result2 = GraphicCTranslator.translate(testFile2, GraphicCTranslator.TYPE_2);
//        System.out.println(result2);
//
//        File testFile3 = new File("E:/JavaProjects/GraphicCR/reserve/GraphicC/1/test/xxxx.jpg");
//        String result3 = GraphicCTranslator.translate("http://jwpt.neuq.edu.cn/ACTIONVALIDATERANDOMPICTURE.APPPROCESS",
//                testFile3, GraphicCTranslator.TYPE_1);
//        System.out.println(result3);
    }
}
