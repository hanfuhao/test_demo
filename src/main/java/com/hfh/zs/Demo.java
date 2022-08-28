package com.hfh.zs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Demo {
    public static void main(String[] args) throws IOException {

        String file=args[0];


        Random rand = new Random();
        int[] bro = new int[]{000123, 453278, 112567, 877966, 342789, 113467, 225567, 334455, 989543, 881234, 667734, 445678, 123956};
        String[] trade_market = new String[]{"1", "2", "3", "4", "5", "Z", "R","1"};
        String[] type = new String[]{"理财", "股票", "债券", "基金", "资产", "咨询","股票","股票"};
        String[] cp = new String[]{"22", "44", "66", "67","74","33","35","24"};
        String[] addr=new String[]{"北京","上海","兰州","郑州","石家庄","贵州","天津","济南"};
        String[] prod_id=new String[]{"1753019","1601727","1601801","1600068","16000291","0002223","0002539","0002031"};
        String[] cplxdm=new String[]{"z03","s00","s10","s30","s40","z31","z50","z60"};
        String[] belong_org_id=new String[]{"ORG-1663","ORG-1679","ORG-1918","ORG-8113","ORG-1630","ORG-1836","ORG-2113","ORG-2612"};
        String[] curr_cd=new String[]{"1","1","1","1","1","0","2","0"};
        String[] trade_board=new String[]{"10","10","00","10","00","10","00","10","00"};
// 流览表
//  表名： bro_table_tmp
//
//  字段：   account_id  String,bro String,market String, even String,type String ,cp String,rate int

        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        for (int i = 30000000; i < 42000000; i++) {
            StringBuffer str1=new StringBuffer();

            for (int j = 1; j <= 200; j++) {
                if(j==200){
                    str1.append( i + "," + trade_market[rand.nextInt(7)] + "," + type[rand.nextInt(7)] + "," + cp[rand.nextInt(7)] + "," + addr[rand.nextInt(7)] + "," + prod_id[rand.nextInt(7)]+"," + cplxdm[rand.nextInt(7)]+"," + belong_org_id[rand.nextInt(7)]+"," + curr_cd[rand.nextInt(7)]+"," + trade_board[rand.nextInt(7)]);
                }else{
                    str1.append( i + "," + trade_market[rand.nextInt(7)] + "," + type[rand.nextInt(7)] + "," + cp[rand.nextInt(7)] + "," + addr[rand.nextInt(7)] + "," + prod_id[rand.nextInt(7)]+"," + cplxdm[rand.nextInt(7)]+"," + belong_org_id[rand.nextInt(7)]+"," + curr_cd[rand.nextInt(7)]+"," + trade_board[rand.nextInt(7)]+",");
                }

            }

            bw.write(str1.toString());
            bw.newLine();
            bw.flush();
        }
        //刷新流
       // bw.flush();
        //释放资源
        bw.close();


    }
}
