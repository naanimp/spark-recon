package com.company.spark.recon.challenges;

import java.util.HashMap;

public class A {
    public int solution(String[] T, String[] R){
        int n = T.length;
        HashMap<String, Boolean> wrong = new HashMap<>();
        HashMap<String, Boolean> all = new HashMap<>();

        for(int i = 0; i < n; i++){
            int len = T[i].length();
            if(T[i].charAt(len-1) >= 'a' && T[i].charAt(len-1) <= 'z'){
                T[i] = T[i].substring(0, len-1);
            }
            if(R[i].equals("OK") == false){
                wrong.put(T[i], true);
            }
            all.put(T[i], true);
        }
        int incorrect = wrong.size();
        return (incorrect*100)/n;
    }
}
