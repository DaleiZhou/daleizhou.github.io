---
layout: post
category: [Coding, POJ]
title: POJ3617 Best Cow Line
excerpt_separator: <!--more-->
---

## 内容 
>Status: Realse

　　今天让我们一起看[POJ3617[1]](http://poj.org/problem?id=3617)吧。
<!--more-->
## <a id="Problem">Problem</a>

### Description

    FJ is about to take his N (1 ≤ N ≤ 2,000) cows to the annual"Farmer of the Year" competition. In this contest every farmer arranges his cows in a line and herds them past the judges.

    The contest organizers adopted a new registration scheme this year: simply register the initial letter of every cow in the order they will appear (i.e., If FJ takes Bessie, Sylvia, and Dora in that order he just registers BSD). After the registration phase ends, every group is judged in increasing lexicographic order according to the string of the initials of the cows' names.

    FJ is very busy this year and has to hurry back to his farm, so he wants to be judged as early as possible. He decides to rearrange his cows, who have already lined up, before registering them.

    FJ marks a location for a new line of the competing cows. He then proceeds to marshal the cows from the old line to the new one by repeatedly sending either the first or last cow in the (remainder of the) original line to the end of the new line. When he's finished, FJ takes his cows for registration in this new order.

    Given the initial order of his cows, determine the least lexicographic string of initials he can make this way.

### Input

    * Line 1: A single integer: N
    * Lines 2..N+1: Line i+1 contains a single initial ('A'..'Z') of the cow in the ith position in the original line

### Output

    The least lexicographic string he can make. Every line (except perhaps the last one) contains the initials of 80 cows ('A'..'Z') in the new line.

### Sample Input

    6
    A
    C
    D
    B
    C
    B

### Sample Output

    ABCBCD

## <a id="Analysis">Analysis</a>

　　这是个涉及字典序的问题，可以用贪心算法来解决。这个问题的题目大意是字符串只从首尾取字母重新组成字符串，要求字典序最小。因为约束了只能从首尾取字符，因此每次都从首尾字符中取出一个较小的放置到新字符串的末尾。

　　这么处理咋一看能解决大部分问题，不过这还不能彻底解决问题，因为首尾字符有可能是相同的，那么从什么地方取出字符呢，答案是不急着决定，继续比较除了这两个字符外的字符串的首尾字符，直到首尾字符不相等，决定出哪一边比较小，那么新字符串填充的字符就从哪边获取，这么做是为了将较小的字符早点露出来，可以早点用上。具体我们来看一个例子说明。

<div align="center">
<img src="/assets/img/2018/10/17/cowline.png" width="50%" height="50%"/>
</div>

　　如上图，假如原始字符串存在data中，值为ACDBCB。结果字符串用result表示，一开始为空。
开头的A和末尾的B比较大小很容易，因此A胜出填入result末尾，如图b)所示。data剩余字符串为CDBCB,首尾比较也很容易角逐出B为胜者添加到result的末尾，如c)。此时data字符串剩余CDBC，现在首尾都是为C,按照上面的思路首尾都跳过当前字符转而比较下一个，此时首尾字符分别是D,B。因此B一边又胜出了，所以将末尾的C添加到result的末尾。接下来data就剩余DBC，后面的步骤步骤赘述，最终得到的result字符串结果为ABCBCD。

　　这个算法过程是个贪心的过程，每步骤求一个局部的优解，最终得到全局的最优解。那么上述算法步骤的贪心过程每次得到的局部最优解组合成的最终结果是否就是全局最优解呢。首尾字符不相等的情况，显然只能取较小的字符。问题就在于首尾字符相等的情况下需要说明一下。根据算法首尾相同情况首尾都需要跳过当前字符转而去比较下一个字符，假如下一个字符也相等则继续都跳过，直到首尾字符不相等(首尾指针相遇情况不会造成特殊情况)，取较小的那一方作为胜出方。去除胜出方那个首字符后露出的字符与败方首字符相比较有三种情况，
1. 如果胜出方露出的字符是小的，符合题目要求，下一步会取这个较小的字符；
2. 如果胜出方露出的字符是大的，那么下一步会将上一步的败方首字符取出来。造成难题的首尾相等字符都被使用了，那么算法跳过首尾相同字符去比较更里面的字符的做法没有带来坏的结果；
3. 如果胜出方露出的字符与败方首字符相同，那么问题又与上一步进行相同的处理，会将问题最终都落在不一样大得字符上，不会超出上面两种情况。

　　因此这里描述的贪心过程得到的局部优解最终组合成全局最优解，下面看AC的代码，注意实现过程中，每个样例输出可能是多行，每行最多80个字符。

## <a id="AC Code">AC Code</a>

```cpp
#include<cstdio>
#include<iostream>
using namespace std;

int main() {
#ifdef POJ_DEBUG_LOCAL
    freopen("in.txt", "r", stdin);
    freopen("out.txt", "w", stdout);
#endif

    int N;
    char temp;
    while (scanf("%d", &N) != EOF) {
        string data(N, '0');
        scanf("%c", &temp);
        for(int i=0; i<N; ++i) {
            scanf("%c", &data[i]);
            scanf("%c", &temp);
        }
        int begin = 0, end = N - 1;
        string result(N, '0');

        while(begin <= end) {
            bool left = false;
            for(int i=0; begin + i <= end; ++i) {
                if(data[begin + i] < data[end - i]) {
                    left = true;
                    break;
                } else if(data[begin + i] > data[end - i]) {
                    break;
                }
            }

            if(left) {
                result[N - 1 - end + begin] = data[begin];
                begin ++;
            } else {
                result[N - 1 - end + begin] = data[end];
                end --;
            }
        }
        
        for(int i=0; i<result.size(); ++i) {
            if(i % 80 == 0 && i != 0) {
                printf("\n");
            }
            printf("%c", result[i]);
        }
        printf("\n");
    }

#ifdef POJ_DEBUG_LOCAL
    fclose(stdin);
    fclose(stdout);
#endif
}
```

## <a id="References">References</a>

* [1] http://poj.org/problem?id=3617