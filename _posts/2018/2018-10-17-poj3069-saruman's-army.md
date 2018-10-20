---
layout: post
category: [Coding, POJ]
title: POJ3069 Saruman's Army
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

　　今天让我们一起看[POJ3069[1]](http://poj.org/problem?id=3069)吧。
<!--more-->
## <a id="Problem">Problem</a>

### Description

    Saruman the White must lead his army along a straight path from Isengard to Helm’s Deep. To keep track of his forces, Saruman distributes seeing stones, known as palantirs, among the troops. Each palantir has a maximum effective range of R units, and must be carried by some troop in the army (i.e., palantirs are not allowed to “free float” in mid-air). Help Saruman take control of Middle Earth by determining the minimum number of palantirs needed for Saruman to ensure that each of his minions is within R units of some palantir.

### Input

    The input test file will contain multiple cases. Each test case begins with a single line containing an integer R, the maximum effective range of all palantirs (where 0 ≤ R ≤ 1000), and an integer n, the number of troops in Saruman’s army (where 1 ≤ n ≤ 1000). The next line contains n integers, indicating the positions x1, …, xn of each troop (where 0 ≤ xi ≤ 1000). The end-of-file is marked by a test case with R = n = −1.

### Output

    For each test case, print a single integer indicating the minimum number of palantirs needed.

### Sample Input

    0 3
    10 20 20
    10 7
    70 30 1 7 15 20 50
    -1 -1

### Sample Output

    2
    4

## <a id="Analysis">Analysis</a>

## <a id="AC Code">AC Code</a>

## <a id="References">References</a>

* [1] http://poj.org/problem?id=3069
