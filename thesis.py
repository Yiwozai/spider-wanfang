#!usr/bin/env python3
# -*- coding: utf-8 -*-

"""
“万方数据”爬虫 —— 期刊部分

Windows分布式客户端
(方便Python打包exe, 集合到一个.py文件)
"""

import re
import os
import sys
import json
import pickle
import socket
import codecs
import logging
import requests
import traceback
import threading
from time import time, sleep
from datetime import datetime
from urllib.parse import urlparse


# 格式化log输出
logging.basicConfig(level=logging.WARNING,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='warning.log',
                    filemode='a')
# logging.getLogger("requests").setLevel(logging.INFO)
# 配置参数
DEFAULT_DELAY = 2
DEFAULT_RETRIES = 2
DEFAULT_THREADS = 2
DEFAULT_TIMEOUT = 20
DEFAULT_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/57.0.2987.133 Safari/537.36'}
domains = {}
crawled_urls = []
all_urls = []
journal_info = []
# 提前编译正则匹配
regex_date = re.compile(r'<a class="(?:\w+?)?" href="(/periodical/\w+/(?:.+?).aspx)">\d{1,2}</a>')
regex_url = re.compile(r'<a class="qkcontent_name" href=\'(.+?)\'>')
regex_dissetation = re.compile(r'<h1 id="title0">(.+?)</h1>', re.S)
regex_dissetation_en = re.compile(r'<h2>(.+?)</h2>', re.S)
regex_doi = re.compile(r'<dt>doi：</dt>(?:.+?)_blank\">(.+?)?</a></dd>', re.S)
regex_abstract = re.compile(r'<t>摘要：</t>(?:.+?)<dd>(.+?)</dd>', re.S)
regex_abstract_en = re.compile(r'<t>Abstract：</t>(?:.+?)<dd>(.+?)</dd>', re.S)
regex_author = re.compile(r'>(\w+?)</a><sup>\[(\d)\]</sup>')
regex_author_en1 = re.compile(r'Author：(?:.+?)<td class="author_td">(.+?)</td>', re.S)
regex_author_en2 = re.compile(r'([A-Z](?:.+?))<sup>\[(\d)\]</sup>')
regex_author_unit1 = re.compile(r'<t>作者单位</t>(.+?)</td>', re.S)
regex_author_unit2 = re.compile(r'<li>(.+?)</li>', re.S)
regex_journal = re.compile(r'<t>刊  名：</t>(?:.+?)>(\w+?)</a>', re.S)
regex_journal_en = re.compile(r'<t>Journal：</t>(?:.+?)>([A-Za-z ]+?)</a>', re.S)
regex_journal_date = re.compile(r'<t>年，卷\(期\)</t>(?:.+?)">(.+?)</a>', re.S)
regex_classification = re.compile(r'<t>分类号</t>(?:.+?)<td>(.+?)</td>', re.S)
regex_keywords1 = re.compile(r'<t>关键词：</t>(.+?)</td>', re.S)
regex_keywords2 = re.compile(r'title=\'(\w+)的知识脉络\'')
regex_keywords_en1 = re.compile(r'<t>Keywords：</t>(.+?)</td>', re.S)
regex_keywords_en2 = re.compile(r'([a-zA-Z][a-zA-Z ]+)</a>')
regex_fund_project = re.compile(r'<t>基金项目</t>(?:.+?)<td>(.+?)</td>', re.S)


class Throttle(object):

    def __init__(self, dealy=DEFAULT_DELAY):
        """这是一个时延类，用于同域名请求的延时
        """
        self.delay = dealy

    def wait(self, url):
        """该类的执行函数，将域名和调用时间以dict的key: value的形式存储与调用
        """
        domain = urlparse(url).netloc
        # 字典的get方法，如果无此key，返回None
        last_accessed = domains.get(domain)
        # is not None: 以前访问过    self.delay > 0： 默认时延大于0
        if last_accessed is not None and self.delay > 0:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                sleep(sleep_secs)
        domains[domain] = datetime.now()


class WanfangSpider(threading.Thread):

    # 文件夹路径，放置在类属性以供访问
    if os.path.exists('D:\\Documents'):
        if not os.path.exists('D:\\Documents\\Spider'):
            os.mkdir('D:\\Documents\\Spider')
        path = 'D:\\Documents\\Spider\\'
    else:
        if not os.path.exists('D:\\Spider'):
            os.mkdir('D:\\Spider')
        path = 'D:\\Spider\\'

    def __init__(self, lock=threading.Lock(), name=None):
        """从Thread继承子类，重写多线程方法
        """
        super(WanfangSpider, self).__init__()
        # 标准线程锁
        self.lock = lock
        self.name = name
        self.refuse = 0
        # 写入文件
        self.file = codecs.open(WanfangSpider.path + 'data.json', 'a', encoding='utf-8')
        # 实例化时延类
        self.throttle = Throttle(DEFAULT_DELAY)
        # 传递响应
        self.response = None
        self.url = None
        # 多线程异常标记
        self.exitcode = 0
        self.exception = None
        self.exc_traceback = ''

    def _parse_dissertation(self):
        """解析论文题目
        """
        try:
            return regex_dissetation.search(self.response.text).group(1).strip()
        except Exception as e:
            logging.error('未找到论文名：' + self.response.url)
            logging.error(str(e))

    def _parse_dissertation_en(self):
        """解析论文英文题目
        """
        try:
            return regex_dissetation_en.search(self.response.text).group(1).strip()
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到论文英文名：' + self.response.url)
            logging.error(str(e))

    def _parse_doi(self):
        """解析doi
        """
        try:
            return regex_doi.search(self.response.text).group(1)
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到doi：' + self.response.url)
            logging.error(str(e))

    def _parse_abstract(self):
        """解析摘要
        """
        try:
            return regex_abstract.search(self.response.text).group(1).strip()
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到论文摘要：' + self.response.url)
            logging.error(str(e))

    def _parse_abstract_en(self):
        """解析英文摘要
        """
        try:
            return regex_abstract_en.search(self.response.text).group(1).strip()
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到论文英文摘要：' + self.response.url)
            logging.error(str(e))

    def _parse_author(self):
        """解析作者名
        """
        try:
            author = regex_author.findall(self.response.text)
            if author:
                string = ''
                for tu in author:
                    for ea in tu:
                        string += ea
                    string += '_'
                return string[:-1]
            else:
                assert author == []
                return None
        except Exception as e:
            logging.error('未找到作者名：' + self.response.url)
            logging.error(str(e))

    def _parse_author_en(self):
        """解析作者英文名
        """
        try:
            lable = regex_author_en1.search(self.response.text).group(1)
            author_en = regex_author_en2.findall(lable)
            if author_en:
                string = ''
                for tu in author_en:
                    for ea in tu:
                        string += ea
                    string += '_'
                return string[:-1]
            else:
                assert author_en == []
                return None
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到作者英文名：' + self.response.url)
            logging.error(str(e))

    def _parse_author_unit(self):
        """解析作者单位
        """
        try:
            lable = regex_author_unit1.search(self.response.text).group(1)
            author_unit = regex_author_unit2.findall(lable)
            if author_unit:
                string = ''
                for unit in author_unit:
                    string += unit.strip()
                return string
            else:
                assert author_unit == []
                return None
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到作者单位：' + self.response.url)
            logging.error(str(e))

    def _parse_journal(self):
        """解析期刊名
        """
        try:
            return regex_journal.search(self.response.text).group(1)
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到期刊名：' + self.response.url)
            logging.error(str(e))

    def _parse_journal_en(self):
        """解析期刊英文名
        """
        try:
            return regex_journal_en.search(self.response.text).group(1)
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到期刊英文名：' + self.response.url)
            logging.error(str(e))

    def _parse_journal_date(self):
        """解析期刊年卷期
        """
        try:
            date = regex_journal_date.search(self.response.text).group(1)
            return date.replace('&nbsp;', '')
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到期刊年卷期：' + self.response.url)
            logging.error(str(e))

    def _parse_classification(self):
        """解析分类号
        """
        try:
            return regex_classification.search(self.response.text).group(1).strip()
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到分类号：' + self.response.url)
            logging.error(str(e))

    def _parse_keywords(self):
        """解析关键词
        """
        try:
            lable = regex_keywords1.search(self.response.text).group(1)
            keywords = regex_keywords2.findall(lable)
            if keywords:
                return '_'.join(keywords)[:-1]
            else:
                assert keywords == []
                return None
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到关键词：' + self.response.url)
            logging.error(str(e))

    def _parse_keywords_en(self):
        """解析英文关键词
        """
        try:
            lable = regex_keywords_en1.search(self.response.text).group(1)
            keywords_en = regex_keywords_en2.findall(lable)
            if keywords_en:
                return '_'.join(keywords_en)[:-1]
            else:
                assert keywords_en == []
                return None
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到英文关键词：' + self.response.url)
            logging.error(str(e))

    def _parse_fund_project(self):
        """解析基金项目
        """
        try:
            return regex_fund_project.search(self.response.text).group(1).strip()
        except AttributeError:
            return None
        except Exception as e:
            logging.error('未找到基金项目：' + self.response.url)
            logging.error(str(e))

    def request(self, url, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT,
                headers=DEFAULT_HEADER):
        """
        request函数重写了requests.get()函数，以便更好的调用与异常控制

        :param url: url
        :param timeout: timeout
        :param retries: 第一次get失败后，将该url记入日志前重新get的次数
        :param headers: 请求头
        :return: class:`Response <Response>` object
        """
        # 时延在这里触发，保证实例化的线程每次请求均受控
        self.throttle.wait(url)
        self.url = url
        # -------------------------------------------------------
        # 请求是最不可控的
        #
        # 这里分了三种情况：
        # 1：请求成功  2：请求超过retries次数  3：请求状态码为302
        # 后两种情况触发时，将该url写入日志，等级为error
        # -------------------------------------------------------
        try:
            r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=False)
        except Exception as e:
            if retries > 1:
                self.request(url, retries-1)
            else:
                logging.error('Failed %d times: %s' % (DEFAULT_RETRIES, url))
                self.response = None
                self.exitcode = 1
                self.exception = e
                self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
                logging.error(self.exc_traceback)
        else:
            if r.status_code == 302:
                logging.error('服务器拒绝: ' + url)
                self.refuse += 1
                self.response = None
            elif r.status_code != 200:
                logging.error('服务器错误: ' + url)
                self.response = None
            else:
                self.response = r

    def parse(self):
        """主功能方法，爬取并保存信息
        """
        # 循环直到urls下载完毕
        while all_urls:
            url = all_urls.pop(0)
            self.request(url)
            if self.response is None:
                self._fail()
                if self.refuse > 3:
                    print('线程拒绝次数超过限制，该线程退出')
                    os._exit(0)
                continue
            # 盛装数据，写入Json
            item = {'url_id': self.response.url.split('_')[1][:-5], 'dissertation': self._parse_dissertation()}
            if item.get('dissertation', None) is None:
                self._fail()
                continue
            item['dissertation_en'] = self._parse_dissertation_en()
            item['doi'] = self._parse_doi()
            item['abstract'] = self._parse_abstract()
            item['abstract_en'] = self._parse_abstract_en()
            item['author'] = self._parse_author()
            item['author_en'] = self._parse_author_en()
            item['author_unit'] = self._parse_author_unit()
            item['journal'] = self._parse_journal()
            item['journal_en'] = self._parse_journal_en()
            item['journal_date'] = self._parse_journal_date()
            item['classification'] = self._parse_classification()
            item['keywords'] = self._parse_keywords()
            item['keywords_en'] = self._parse_keywords_en()
            item['fund_project'] = self._parse_fund_project()
            for key in item.copy():
                if item[key] is None:
                    del item[key]
            line = json.dumps(item, ensure_ascii=False) + '\n'
            self.file.write(line)
            crawled_urls.append(self.url)
            self._succeed()

    def run(self):
        """重写run方法，定义功能入口
        """
        try:
            self.parse()
        except Exception as e:
            self.exitcode = 1
            self.exception = e
            self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            logging.error(self.exc_traceback)
        else:
            self.file.close()

    def _fail(self):
        """
        一共有三个文件系统记录urls相关：

        allurls.pickle记录期刊总，本地使用
        crawledurls.pickle记录已爬取，本地使用
        failed.txt收集错误url，上传服务器

        该方法收集错误url
        """
        self.lock.acquire()
        with open(self.path + 'failed.txt', 'a') as ff:
            ff.write(self.url.strip() + '\n')
        self.lock.release()

    def _succeed(self):
        """更新过程url
        """
        self.lock.acquire()
        with open(self.path + 'crawledurls.pickle', 'wb+') as pf:
            pickle.dump(crawled_urls, pf, pickle.HIGHEST_PROTOCOL)
        self.lock.release()


class FirstSpider(WanfangSpider):

    def __init__(self):
        """继承子类,用于urls的初始获取
        """
        super(FirstSpider, self).__init__()
        self.file.close()

    def conntact(self):
        """连接服务器，获取任务
        """
        host = '47.94.139.196'
        port = 21567
        bufsiz = 1024
        addr = (host, port)

        tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_client.connect(addr)

        try:
            tcp_client.send(b'\x00')
            task = tcp_client.recv(bufsiz).decode('utf-8')
            tcp_client.close()
            return json.loads(task)
        # 服务器拒绝的错误直接捕获，提示稍后重连
        except ConnectionRefusedError as e:
            input('服务器错误，请稍后重连...')
            raise e
        except Exception as e:
            print(str(e))
            tcp_client.close()
            # 错误重连
            self.conntact()

    def _urls_get(self, url):
        self.request(url)
        assert self.response is not None, 'FirstSpider无法获得响应'
        page_urls = regex_url.findall(self.response.text)
        assert page_urls != [], '存在的期刊，不应该没有文章 ' + url
        all_urls.extend(page_urls)
        self.lock.acquire()
        with open(self.path + 'allurls.pickle', 'wb+') as pf1:
            pickle.dump(all_urls, pf1, pickle.HIGHEST_PROTOCOL)
        self.lock.release()

    def urls_get(self):
        """写入allurls.pickle
        """
        self.request(journal_info[1])
        assert self.response is not None, 'FirstSpider无法获得响应'
        date_urls = regex_date.findall(self.response.text)
        nums = 0
        for date_url in date_urls:
            line = str(nums) + '/' + str(len(date_urls))
            print(line, end='')
            print('\b' * len(line), end='', flush=True)
            date_url = 'http://c.g.wanfangdata.com.cn' + date_url
            self._urls_get(date_url)
            nums += 1

        # 简单标记allurls爬取完毕
        with open(self.path + 'tag', 'wb+') as tf:
            tf.write(b'\x00')

    def _journal_info_get(self):
        journal_info.clear()
        try:
            if not os.path.exists(self.path + 'info'):
                info = self.conntact()
                inf = open(self.path + 'info', 'w+')
                inf.write(json.dumps(info))
                inf.close()
            else:
                inf = open(self.path + 'info', 'r')
                info = json.loads(inf.read())
                inf.close()
            journal_info.extend(info)
            print('服务器任务：%s/8127' % journal_info[2])
        except TypeError:
            os.remove(self.path + 'info')
            self._journal_info_get()
        finally:
            pass

    def _cache(self):
        """读取缓存
        """
        try:
            # 读取allurls
            with open(self.path + 'allurls.pickle', 'rb') as p_f1:
                temp_all_urls = pickle.load(p_f1)

            # 读取crawledurls，allurls与其做差集
            with open(self.path + 'crawledurls.pickle', 'rb') as p_f2:
                temp_crawled_urls = pickle.load(p_f2)
                crawled_urls.extend(temp_crawled_urls)
            temp_all_urls = list(set(temp_all_urls).difference(set(temp_crawled_urls)))
            all_urls.extend(temp_all_urls)

            # # 读取failed中url，如果存在
            # if os.path.exists(self.path + 'failed.txt'):
            #     with open(self.path + 'failed.txt', 'r') as ff1:
            #         for line in ff1:
            #             all_urls.append(line)
        # 可能文件丢失
        except FileNotFoundError:
            self._nocache()
        # 显示继续下载信息
        else:
            print('继续下载...\n')

    def _nocache(self):
        """没有缓存，全新下载
        """
        try:
            self.urls_get()
        # 服务器拒绝的错误直接捕获，提示稍后重连
        except ConnectionRefusedError as e:
            input('服务器错误，请稍后重连...')
            raise e
        except Exception as e:
            self.exitcode = 1
            self.exception = e
            self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
            logging.error(self.exc_traceback)
            print('发生关键错误，程序重新开始...\n')
            main()

    def run(self):
        self._journal_info_get()
        # 有缓存
        if os.path.exists(self.path + 'tag'):
            self._cache()
        # 全新下载
        else:
            self._nocache()


class SpiderProcessBar(threading.Thread):

    def __init__(self, maxval, union, inival=0):
        super(SpiderProcessBar, self).__init__()
        self.starttime = time()
        self.max = maxval
        self.union = union
        self.status = inival

        self.now = len(union)
        self.interval = 1

    def _cout(self):
        per, num = self._counter()
        line_p = '>' * num
        line_p = line_p.ljust(50)
        line_p = '[' + line_p + ']'
        line_l = '%.2f' % per + '%'
        line = line_p + '   ' + line_l
        print(line, end='')
        print('\b' * len(line), end='', flush=True)

    def _counter(self):
        self.now = len(self.union)
        percent = self.now / self.max * 100
        number = int(int(percent + 0.5) / 2)
        tup = (percent, number)
        return tup

    def _speed(self):
        pass

    def _forecast(self):
        """后续可加入剩余时间
        """
        pass

    def run(self):
        while True:
            sleep(self.interval)
            self._cout()
            if self.now == self.max:
                print('\n')
                break


class Connection(object):
    """
    与服务器通信的客户端类

    事务简略过程：
    服务器：(内心活动：你想干什么呢？)    客户端：1）获取任务 2）上传数据
    服务器：1）你等等, 我找个给你。 来, 这是你的任务, 拿好。  客户端：好嘞，再见。
    服务器：2）我准备好了, 你开始上传吧                     客户端：传完了，再见。
    """

    def __init__(self, ins, name=None, dirpath=''):
        """
        初始化套接字信息, 并调用相应方法

        :param ins: 标明此次任务是获取任务还是上传数据
        :param name: 上传数据建立的文件名
        :param dirpath: 文件夹地址
        """
        self.host = '47.94.139.196'
        self.port = 21567
        self.bufsiz = 1024 * 4
        self.addr = (self.host, self.port)

        self.ins = ins
        self.name = name
        self.dirpath = dirpath

    def _link(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(self.addr)
        # 服务器拒绝的错误直接捕获，提示稍后重连
        except ConnectionRefusedError as e:
            input('服务器错误，请稍后重连...')
            raise e
        except Exception as e:
            print(str(e))
            print('服务器连接错误, 重新连接...')
            self._link()

    def _ack(self):
        """TCP连接中状态确认，该方法用于单个信号发送后，确认服务器准备就绪且正确的状态
        """
        ack = self.sock.recv(self.bufsiz)
        if ack != b'\x02':
            print('检测到状态错误, 尝试重传...')
            self.sock.close()
            self.upload()

    def assign_task(self):
        """分配任务
        """
        try:
            self._link()
            self.sock.send(self.ins)
            task = self.sock.recv(self.bufsiz).decode('utf-8')
            self.sock.close()
            return json.loads(task)
        # 服务器拒绝的错误直接捕获，提示稍后重连
        except ConnectionRefusedError as e:
            input('服务器错误，请稍后重连...')
            raise e
        except Exception as e:
            print(str(e))
            print('任务分配错误, 重新连接...')
            self.assign_task()

    def upload(self):
        """上传数据
        """
        try:
            self._link()
            self.sock.send(self.ins)
            self._ack()
            print('正在上传数据...\n')

            bname = self.name.encode('utf-8')
            self.sock.send(bname)
            self._ack()

            # 数据主体传送
            f = open(self.dirpath + 'data.json', 'rb')
            while True:
                streamdata = f.read(self.bufsiz)
                if not streamdata:
                    break
                self.sock.sendall(streamdata)
            f.close()

            sleep(0.5)
            self.sock.send(b'EOF')
            self._ack()

            # 确认数据发送完毕，并指明是否有failed urls
            if os.path.exists(self.dirpath + 'failed.txt'):
                self.sock.send(b'\x03\x03')  # 有
                with open(self.dirpath + 'failed.txt', 'rb') as f:
                    while True:
                        streamdata = f.read(self.bufsiz)
                        if not streamdata:
                            break
                        self.sock.sendall(streamdata)

                    sleep(0.5)
                    self.sock.send(b'EOF')
            else:
                self.sock.send(b'\x03\x04')  # 无

            # 完整的过程后，关闭连接
            self.sock.close()

        # 服务器拒绝的错误直接捕获，提示稍后重连
        except ConnectionRefusedError as e:
            input('服务器错误，请稍后重连...')
            raise e

        except Exception as e:
            print(str(e))
            print('检测到数据传输错误, 尝试重传...')
            self.sock.close()
            self.upload()

        # 上传成功， 清除文件
        else:
            if self.ins == b'\x00\x00':
                file = [self.dirpath + filename for filename in
                        ('tag', 'data.json', 'info', 'allurls.pickle', 'crawledurls.pickle', 'failed.txt')]
                try:
                    for i in file:
                        os.remove(i)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print(str(e))
                    pass


def timer(func):
    """装饰器，输出运行时间
    """
    def wrapper(*args, **kwargs):
        start_time = time()
        func(*args, **kwargs)
        end_time = time()
        use_time = int(end_time - start_time)

        if use_time > 3600:
            hour = int(use_time / 3600)
            mins = int((use_time % 3600) / 60)
            secs = int(use_time - hour * 3600 - mins * 60)
            print('用时: %d时%d分%d秒' % (hour, mins, secs))

        elif use_time > 60:
            mins = int(use_time / 60)
            secs = int(use_time - mins * 60)
            print('用时: %d分%d秒' % (mins, secs))

        else:
            print('用时: %d秒' % use_time)
    return wrapper


@timer
def start():
    """主函数，逻辑体现
    """
    # 该分布式爬虫客户端首先实例化FirstSpider类，
    # 该类单线程获取一个期刊内，各期数中的所有论文链接并放入urls
    # 该变量在运行时存在, 并写入allurls.pickle持久化
    try:
        page_thread = FirstSpider()
        page_thread.start()
        page_thread.join()
    except KeyboardInterrupt:
        sys.exit(0)
    # 捕获到关键异常，全部重新开始
    except Exception as e:
        logging.error('FirstSpider获取失败：' + str(e))
        print('发生关键错误，程序重新开始...\n')
        main()

    if len(all_urls) == 1:
        with open(WanfangSpider.path + 'failed.txt', 'a') as ff:
            ff.write(all_urls[0].strip() + '\n')
        crawled_urls.append(all_urls[0])
        all_urls.clear()

    # urls获取完毕
    # 然后实例WanfangSpider，线程数为设置数量
    # 该类所有线程循环获取urls中url，同时pop该url，urls数量减一
    # 成功返回到data.json，或者请求失败。失败者，写入日志；成功者，计入crawledurls.pickle
    # 直至urls为空
    print('--- 正式爬取 ---')
    threads = []
    for i in range(DEFAULT_THREADS):
        t = WanfangSpider(name='wanfang')
        threads.append(t)
    # lenth = len(all_urls) + len(crawled_urls)
    # t = SpiderProcessBar(maxval=lenth, union=crawled_urls, inival=len(crawled_urls))
    # threads.append(t)
    try:
        for i in threads:
            i.start()
        for i in threads:
            i.join()
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logging.error('子线程错误：' + str(e))


def main():
    while True:
        start()
        Connection(ins=b'\x00\x00', name=journal_info[0], dirpath=WanfangSpider.path).upload()
        lenth = len(all_urls) + len(crawled_urls)
        all_urls.clear()
        crawled_urls.clear()
        print('您此次贡献了%d条数据' % lenth)

        scores = 0
        with open(WanfangSpider.path + 'scores', 'a') as sf:
            sf.write(str(lenth) + '\n')
        with open(WanfangSpider.path + 'scores', 'r') as sf:
            for scoreline in sf:
                scores += int(scoreline)
        print('您一共贡献了%d条数据, 感谢!' % scores)

        print('\n程序将在3秒后继续运行...\n')
        sleep(3)


if __name__ == '__main__':
    print('感谢协助爬取数据, 程序运行中可随时退出')
    print('------------------------------------------\n')
    main()
