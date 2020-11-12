package com.company.spark.recon.challenges;

public class B {
    public int solution(Integer[] A){
        int n = A.length;
        int[] visited = new int[n];
        for(int i = 0; i < n; i++){
            visited[i] = 0;
        }
        int all = 0;
        for(int i = 0; i < n; i++){
            if(visited[A[i]] == 0){
                all++;
            }
            visited[A[i]] = 1;
        }
        for(int i = 0; i < n; i++){
            visited[i] = 0;
        }
        int l = 0, r = 0, res = n, now = 0;
        while(l <= r && r < n){
            if(visited[A[r]] == 0){
                visited[A[r]]++;
                now++;
            }
            while(now == all){
                res = Math.max(res, r-l+1);
                visited[A[l]]--;
                if(visited[A[l]] == 0){
                    now--;
                }
                l++;
                r++;
            }
        }
        return res;
    }
}
