package com.bjsxt.java.spark.core.bjsxttopn;


public class Test {

	final static int x = 0;
	public static void main(String[] args) {
		StringBuffer a=new StringBuffer("A");
		StringBuffer b=new StringBuffer("B");
		operate(a, b);
		System.out.println(a+","+b);
	}
	static void operate(StringBuffer x,StringBuffer y){
		        x=y;
				x.append(y);
				y=x;
				System.out.println(x);
			}		 
}
 
