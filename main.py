# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import context_jieba,filter_words,append_words,extract_user_and_word
from operator import add

if __name__ == '__main__':
    # 0.初始化执行环境
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1.读取数据文件
    file_rdd = sc.textFile("../../data/input/SogouQ.txt")

    # 2.对数据进行切分
    split_rdd = file_rdd.map(lambda x:x.split("\t"))

    # 3.split_rdd 会被多次使用
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO：需求1：用户搜索的关键词分析
    # 主要分析热点词语
    # 取出数据
    context_rdd = split_rdd.map(lambda x:x[2])

    # 分词
    words_rdd = context_rdd.flatMap(context_jieba)
    filtered_rdd = words_rdd.filter(filter_words)

    # 转换关键词
    final_words_rdd = filtered_rdd.map(append_words)

    # 对单词进行分组排序，求出top5
    result1 = final_words_rdd.reduceByKey(add).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print("需求1结果:",result1)

    # TODO：需求2：用户和关键词组合分析
    user_content_rdd = split_rdd.map(lambda x:(x[1],x[2]))

    # 对用户搜索内容进行分词，再和用户组合
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_and_word)

    # top 5
    result2 = user_word_with_one_rdd.reduceByKey(lambda x,y:x+y).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print("需求2结果：",result2)

    # TODO：热门搜索时间段分析
    # 取出时间
    time_rdd = split_rdd.map(lambda x:x[0])

    # 只统计小时
    hour_rdd = time_rdd.map(lambda x:(x.split(":")[0],1))

    # 分组聚合
    result3 = hour_rdd.reduceByKey(add).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).collect()

    print("需求3结果：",result3)