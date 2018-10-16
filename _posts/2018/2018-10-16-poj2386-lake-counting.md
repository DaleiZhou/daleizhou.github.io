---
layout: post
category: [Coding, POJ]
title: POJ 2386 Lake Counting
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

　　今天让我们一起看[POJ2386[1]](http://poj.org/problem?id=2386)吧。
<!--more-->
## <a id="Problem">Problem</a>

### Description

    Due to recent rains, water has pooled in various places in Farmer John's field, which is represented by a rectangle of N x M (1 <= N <= 100; 1 <= M <= 100) squares. Each square contains either water ('W') or dry land ('.'). Farmer John would like to figure out how many ponds have formed in his field. A pond is a connected set of squares with water in them, where a square is considered adjacent to all eight of its neighbors. 

    Given a diagram of Farmer John's field, determine how many ponds he has.

### Input

    Line 1: Two space-separated integers: N and M 

    Lines 2..N+1: M characters per line representing one row of Farmer John's field. Each character is either 'W' or '.'. The characters do not have spaces between them.

### Output

    Line 1: The number of ponds in Farmer John's field.

### Sample Input

    10 12
    W........WW.
    .WWW.....WWW
    ....WW...WW.
    .........WW.
    .........W..
    ..W......W..
    .W.W.....WW.
    W.W.W.....W.
    .W.W......W.
    ..W.......W.

### Sample Output

    3

## <a id="Analysis">Analysis</a>

　　这是一道简单的搜索的题，对于搜索来说一般DFS,BFS都是可以解决的，但是其实也是有区别的。DFS往往通过递归实现的方式，不显式地使用栈来记录状态,管理起来比较简单，而BFS往往实现起来需要自己维护两个队列轮换读取和写入。当然BFS也是有优势的，BFS

## <a id="AC Code">AC Code</a>    

## <a id="References">References</a>

* [1] http://poj.org/problem?id=2386
