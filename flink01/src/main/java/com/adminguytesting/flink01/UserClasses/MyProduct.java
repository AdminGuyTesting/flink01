package com.adminguytesting.flink01.UserClasses;

public class MyProduct {
    int price1;
    String name1;
    int price2;
    String name2;

    public MyProduct(int a, int b, String s1, String s2) {
        this.name1=s1;
        this.name2=s2;
        this.price1=a;
        this.price2=b;
    }
    public void setPrice1(int p) {price1=p;}
    public void setPrice2(int p) {price2=p;}
    public void setName1(String name) {this.name1 = name;}
    public void setName2(String name) {this.name2 = name;}

    public int getPrice1() {return price1;}
    public int getPrice2() {return price2;}
    public String getName1() {return name1;}
    public String getName2() {return name2;}
}
