---
layout: post
category: [Coding, POJ]
title: POJ1325 Machine Schedule
excerpt_separator: <!--more-->
---


## 内容 
>Status: Release

　　数据结构与算法部分已经久未练习业已生疏，在业余时间带着刷一些题将技能捡起来。这个系列从[Poj1325[1]](http://poj.org/problem?id=1325)开始吧。
<!--more-->
## <a id="Problem">Problem</a>

### Description

    As we all know, machine scheduling is a very classical problem in computer science and has been studied for a very long history. Scheduling problems differ widely in the nature of the constraints that must be satisfied and the type of schedule desired. Here we consider a 2-machine scheduling problem. 

    There are two machines A and B. Machine A has n kinds of working modes, which is called mode_0, mode_1, ..., mode_n-1, likewise machine B has m kinds of working modes, mode_0, mode_1, ... , mode_m-1. At the beginning they are both work at mode_0. 

    For k jobs given, each of them can be processed in either one of the two machines in particular mode. For example, job 0 can either be processed in machine A at mode_3 or in machine B at mode_4, job 1 can either be processed in machine A at mode_2 or in machine B at mode_4, and so on. Thus, for job i, the constraint can be represent as a triple (i, x, y), which means it can be processed either in machine A at mode_x, or in machine B at mode_y. 

    Obviously, to accomplish all the jobs, we need to change the machine's working mode from time to time, but unfortunately, the machine's working mode can only be changed by restarting it manually. By changing the sequence of the jobs and assigning each job to a suitable machine, please write a program to minimize the times of restarting machines. 

### Input

    The input file for this program consists of several configurations. The first line of one configuration contains three positive integers: n, m (n, m < 100) and k (k < 1000). The following k lines give the constrains of the k jobs, each line is a triple: i, x, y. 

    The input will be terminated by a line containing a single zero. 

### Output

    The output should be one integer per line, which means the minimal times of restarting machine.

### Sample Input

    5 5 10
    0 1 1
    1 1 2
    2 1 3
    3 1 4
    4 2 1
    5 2 2
    6 2 3
    7 2 4
    8 3 3
    9 4 3
    0

### Sample Output

    3

## <a id="Analysis">Analysis</a>

　　我们将每个mode看成一个个点，而每个job可以看成A或B的状态集的连线。因为不包含奇数条边的回路。因此这个图是一个二分图。因为所有任务都得完成，因此这是一个求二分图最小覆盖的问题。即找出最小点覆盖将所有任务边都能覆盖到。而最小点覆盖数等于最大匹配数(Konig 定理)，因此这个问题可以通过求此图的最大匹配来解决。

　　匈牙利算法是解决最大匹配问题的一个解决方案。其算法的主要过程是从一个未匹配点出发，依次经过未匹配边，匹配边...未匹配边，匹配边，最后经过一条未匹配边到达一个未匹配点，亦即名为`增广路`。从一个未匹配点出发，遍历所连接的另一部中的点。另一部连接点有两种情况，一种是未匹配点，则将连线加入匹配中。若另一边的点为匹配点，则通过这个匹配点看能否找到一条增广路到另一个未匹配点。若存在一条增广路，则将这条增广路中非匹配边变为匹配边加入进匹配中，将已匹配边从匹配中去掉。根据增广路的性质，这么操作之后匹配集中就多一条匹配。若遍历另一部中的点都没有找到上述情况则在本部中检查下一个点，继续算法的迭代。下面我们看一个具体的例子：

<div align="center">
<img src="/assets/img/2018/10/12/hungary.png" width="60%" height="60%"/>
</div>

　　题目初始化的情况如图中0所示，此时匹配为空。现在遍历**1**点所连接的点，一开始就找到未匹配点**a**，则将(1,a)加入匹配中去处理**2**点。同样地(2,c)被加入匹配中，跳过**2**点处理**3**点。如图中3所示，遍历3点连接的另一部点集合中找到a，而a已经在图中1中实现匹配，因此算法就变为能否从**3**出发途经点**a**找到一条增广路。此时**a**的匹配点为点**1**，则检查1是否有连接的未访问的非匹配边，这里找到了边(1, b), 即从3出发找到一条增广路，途经`3->a->1->b`,其中边(1,a)已在匹配中，因此将(1,a)从匹配中去掉，加入(3,a),(1,b)，此时匹配情况如图中4所示。进而跳过**3**去处理**4**，。情况与点**3**类似，不再赘述。

　　注意到两台机器的初始工作模式都为mode_0, 因此输入数据时需要吧这部分数据都排除在外。即除了mode_0外，求最小覆盖集。

## <a id="AC Code">AC Code</a>
```cpp
#include<cstdio>
#include<vector>
#include<algorithm>

using namespace std;

vector<int> matrix[100];
int match[100];
bool visited[100];

bool dfs(int from) {
    for(int i=0; i<matrix[from].size(); ++i) {
        int to = matrix[from][i];
        if(visited[to]) continue;
        visited[to] = true;

        // 如果to尚未匹配
        // 或to已经匹配，通过to找增广路
        if(match[to] == -1 || dfs(match[to])) {
            match[to] = from;
            return true;
        }
    }

    return false;
}

int main() {
#ifdef POJ_DEBUG_LOCAL
    freopen("in.txt", "r", stdin);
    freopen("out.txt", "w", stdout);
#endif

    int n, m, k;
    while (scanf("%d", &n) && n) {
        for (int i = 0; i < n; ++i) matrix[i].clear();

        scanf("%d %d", &m, &k);
        int i, x, y;
        for (int j = 0; j < k; ++j) {
            scanf("%d %d %d", &i, &x, &y);
            if (!x || !y) continue;
            matrix[x].push_back(y);
        }
        int result = 0;
        fill(match, match+m, -1);

        for (int i = 0; i < n; ++i) {
            fill(visited, visited+m, false);
            if (dfs(i) == true) ++result;
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

* [1] http://poj.org/problem?id=1325