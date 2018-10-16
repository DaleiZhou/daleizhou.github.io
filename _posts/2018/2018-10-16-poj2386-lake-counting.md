---
layout: post
category: [Coding, POJ]
title: POJ 2386 Lake Counting
excerpt_separator: <!--more-->
---

## 内容 
>Status: Realse

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

　　这个题目面试笔试题经常遇到，记得那时15年找工作的时候微软的电话面试面了我这道题，再那之前其实这题在LeetCode上都刷了N遍，压抑内心的狂喜抖着手都把答案通过邮件发过去，然后对方说你这里有个Bug……确实是手一抖写了个Bug，微软的门就关了，由于Bug确实太低级了，我都不好意思说出来。

　　这是一道简单的搜索的题，对于搜索来说一般DFS,BFS都是可以解决的，二者在实现上是有区别的。DFS往往通过递归实现的方式，不用显式地使用栈来记录状态,管理起来比较简单。而BFS往往实现起来需要自己维护两个队列轮换读取和写入。当然BFS也是有优势的，BFS对于同一个状态只经历一次，而在有的问题下DFS需要反复经过相同的状态。BFS的空间复杂度与状态总数是成正比的，而DFS空间复杂度与深度是成正比的，因此往往DFS相较于BFS更节省内存。不过就像上面说的那样，因为DFS可以通过递归+隐式栈使得代码更简短，所以往往还是倾向于用DFS实现本题。

　　下面回到问题本身上来，这个题确实比较简单，遍历field:
1. 如果对应位置为`W`,则将所在的点替换为`.`，并依次检查周围八个点(在field范围内的)是否为`W`,如果是则通过DFS将邻接的`W`都替换成`.`。一次DFS之后就将从原始的W开始的连接区域中W都被标记为`.`了。最终结果计数器加1。
2. 如果对应位置为`.`,则跳过继续检查下一个点。

## <a id="AC Code">AC Code</a>

```cpp
#include<cstdio>
#include<vector>
#include<algorithm>
#include<climits>
#include<iostream>
using namespace std;

int dx[] = {-1, -1, -1, 0, 0, 1, 1, 1};
int dy[] = {-1, 0, 1, -1, 1, -1, 0, 1};


void dfs(int N, int M, int x, int y, vector<vector<char> >& data) {
    data[x][y] = '.';

    for(int i=0; i<8; ++i) {
        int newX = x + dx[i], newY = y + dy[i];

        if(newX >= 0 && newX < N && newY >= 0 && newY < M && data[newX][newY] == 'W') {
            dfs(N, M, newX, newY, data);
        }
    }
}


int main() {
#ifdef POJ_DEBUG_LOCAL
    freopen("in.txt", "r", stdin);
    freopen("out.txt", "w", stdout);
#endif

    int N, M;
    char temp;
    while (scanf("%d %d", &N, &M) != EOF) {
        vector<vector<char> > data(N, vector<char>(M));
        scanf("%c", &temp);
        for(int i=0; i<N; ++i) {
            for(int j=0; j<M; ++j) {
                scanf("%c", &temp);
                data[i][j] = temp;
            }
            scanf("%c", &temp);
        }


        int result = 0;
        for(int i=0; i<N; ++i) {
            for(int j=0; j<M; ++j) {
                if(data[i][j] == 'W') {
                    dfs(N, M, i, j, data);
                    result ++;
                }
            }
        }
        printf("%d\n", result);
    }

#ifdef POJ_DEBUG_LOCAL
    fclose(stdin);
    fclose(stdout);
#endif
}
```

## <a id="References">References</a>

* [1] http://poj.org/problem?id=2386
