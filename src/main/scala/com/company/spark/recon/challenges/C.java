package com.company.spark.recon.challenges;

import java.util.HashMap;

public class C {
    public int solution(int[] A, int S){
        int n = A.length;
        long[] B = new long[n];

        for(int i = 0; i < n; i++){
            B[i] = (long)A[i] - (long)S;
        }

        for(int i = 1; i < n; i++){
            B[i] += B[i-1];
        }

        HashMap<Long, Integer> count = new HashMap<>();
        long res = 0;
        count.put(0L, 1);

        for(int i = 0; i < n; i++){
            int c =  count.getOrDefault(B[i], 0);
            res += c;
            count.put(B[i], c + 1);
        }
        res = Math.min(res, 1000000000);
        return (int)res;
    }
}
