package com.company.spark.recon;

import java.util.HashMap;
import java.util.Map;

class Solution2 {
    public int solution(int[] H) {
        // write your code in Java SE 8
        int gMin = H[0],gMax=H[0];
        Map<Integer,Integer> map = new HashMap<>();
         int count = 1;
        int i =1;

        while(i < H.length) {
//        if(H[i] < gMin){
//            gMin = H[i];
//        }

        if(H[i] > gMax){
            map.put(i,gMax);
            gMax = H[i];
              }
            map.put(i,gMax);

            i++;
        }
        i =0; int j=0;
//        while(i < H.length) {
        for(int key : map.keySet()){
            System.out.println(key + " "+map.get(key));
//            if(map.get(i) != null) {
//                count = map.get(i) * ((i) - j) ;
            }
//            System.out.println(i + "  " + map.get(i));
//        }

        return 1;
    }
    public static void main(String[] args) {
        Solution2 s = new Solution2();
        int a[] =  {1, 1, 7, 6, 6, 6} ; //H = [1, 1, 7, 6, 6, 6] 30
        System.out.println(s.solution(a));
    }
}



