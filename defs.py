# coding:utf8
import jieba


def context_jieba(data):
    seg = jieba.cut_for_search(data)
    l = []

    for word in seg:
        l.append(word)
    return l


def filter_words(data):
    """过滤谷，帮，课"""
    return data not in {"谷", "帮", "课"}


def append_words(data):
    "修订某些关键词"
    if data == '传智博 ':
        data = '传智博客'
    if data == '院校 ':
        data = '院校帮'
    if data == '博学 ':
        data = '博学谷'
    return (data, 1)


def extract_user_and_word(data):
    user_id = data[0]
    user_content = data[1]

    words = context_jieba(user_content)

    return_list = []
    for word in words:
        if filter_words(word):
            return_list.append((user_id+"_"+append_words(word)[0],1))
    return return_list

