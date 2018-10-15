---
layout: post
category: Coding
title: POJ2479 Maximum sum
excerpt_separator: <!--more-->
---


## 内容 
>Status: Draft

　　今天做一道入门级DP的题，出自[POJ2479](http://poj.org/problem?id=2479)
<!--more-->
## <a id="Problem">Problem</a>

### Description

    Given a set of n integers: A={a1, a2,..., an}, we define a function d(A) as below:

$$\max \limits_{1\leq s_{1} \leq t_{1} < s_{2} \leq t_{2} \leq n } \{ \sum_{i=s_{1}}^{t_{1}} a_{i} + \sum_{j=s_{2}}^{t_{2}} a_{j} \}$$

    Your task is to calculate d(A).

### Input

    The input consists of T(<=30) test cases. The number of test cases (T) is given in the first line of the input. 
    Each test case contains two lines. The first line is an integer n(2<=n<=50000). The second line contains n integers: a1, a2, ..., an. (|ai| <= 10000).There is an empty line after each case.

### Output

    Print exactly one line for each test case. The line should contain the integer d(A).

### Sample Input

    1

    10
    1 -1 2 2 3 -3 4 -4 5 -5

### Sample Output

    13

## <a id="Analysis">Analysis</a>

　　首先是对问题的理解，从公式中我们可以看到d(A)代表给定的数组中两段不想交的连续子数组和最大的值。他的基础款大家一定是见过的，那就是面试中经常出现的求连续子数组最大和的题[[2]](https://blog.csdn.net/qq_34528297/article/details/72700695)。而这个基础款可以用动态规划来解决：

　　假如数组的数据存在名为data的数组中，现申请一个数组命名为dp，dp中的元素dp[i]表示以i为结尾的连续子数组和最大的值。状态转移方程为：\\( dp_{i} = \max \( dp_{i-1} + data_{i}, data_{i} \) \\),即数组data从头到尾遍历一遍，当遍历到位置i时,比较dp[i-1] + data[i]与data[i]的大小，将较大的值填入dp[i]的位置。因此最终结果即遍历一遍dp数组求最大值即可。而在实际编程过程中dp数组也不是必须的，只需遍历原数组data一边更新状态一边更新全局结果，可以做到O(n)时间复杂度和O(1)空间复杂度来进行求解。

<div align="center">
<img src="/assets/img/2018/10/14/dp.png" width="40%" height="40%"/>
</div>


　　在这个基础上再看POJ2479就很简单了，利用分治的思路，位置i将原数组切割成左右两个数组，在左右两个数组分别求上述基础题的结果，左右两边结果加起来即为d(A)_i。实际实现过程中，维护两个数组max_l和max_r, 里面的元素分别以位置i为分割点得到的左右两个数组最大连续子数组和，遍历i可以遍历所有可能。最后再遍历一遍i从1到n-1,求 result = max(result, max_l[i-1], max_r[i]), 遍历结束result即为最终结果。

## <a id="AC Code">AC Code</a> 

```cpp
#include<cstdio>
#include<vector>
#include<algorithm>
#include<climits>

using namespace std;

int dp(vector<int>& data) {
    if(data.size() == 0) return 0;

    vector<int> max_l(data.size());
    int dp = max_l[0] = data[0];
    for(int i = 1; i < data.size(); ++i) {
        dp = max(dp + data[i], data[i]);
        max_l[i] = max(max_l[i - 1], dp);
    }

    int result = INT_MIN;
    int max_r = dp = data[data.size() - 1];
    for(int i = data.size() - 2; i >= 0; --i) {
        dp = max(dp + data[i], data[i]);
        result = max(max_r + max_l[i], result);
        max_r = max(max_r, dp);
    }

    return result;
}

int main() {
#ifdef POJ_DEBUG_LOCAL
    freopen("in.txt", "r", stdin);
    freopen("out.txt", "w", stdout);
#endif

    int T, n, temp;
    while (scanf("%d", &T) != EOF) {
        while(T--) {
            scanf("%d", &n);
            vector<int> data;
            while(n--) {
                scanf("%d", &temp);
                data.push_back(temp);
            }
            int result = dp(data);
            printf("%d\n", result);
        }
    }

#ifdef POJ_DEBUG_LOCAL
    fclose(stdin);
    fclose(stdout);
#endif
}
```

## <a id="References">References</a>

* [1] http://poj.org/problem?id=2479

* [2] https://blog.csdn.net/qq_34528297/article/details/72700695

