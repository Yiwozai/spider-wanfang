#!usr/bin/env python3
# -*- coding: utf-8 -*-

"""
分布式客户端所连接的服务器

负责分发任务和接收数据
"""


import os
import json
from socket import *


HOST = ''
PORT = 21567
BUFSIZ = 1024 * 4
ADDR = (HOST, PORT)


def update():
    t = urls.popitem()
    with open('nowtasks.json', 'w+', encoding='utf-8') as nf1:
        nf1.write(json.dumps(urls))
    return t


def read_update():
    with open('nowtasks.json', 'r', encoding='utf-8') as nf2:
        data = nf2.read()
    return json.loads(data)


# 事务简略过程：
# 服务器：(内心活动：你想干什么呢？)    客户端：1）获取任务 2）上传数据
# 服务器：1）你等等，我找个给你。 来，这是你的任务，拿好。  客户端：好嘞，再见。
# 服务器：2）我准备好了，你开始上传吧                     客户端：传完了，再见。


def service():
    sersock = socket(AF_INET, SOCK_STREAM)
    sersock.bind(ADDR)
    sersock.listen(5)

    while True:
        print('waiting for connection...')
        clisock, addr = sersock.accept()
        print('...connected from: ', addr)

        while True:
            ins = clisock.recv(BUFSIZ)
            # --------------------
            # print('指令一执行')
            # --------------------
            if ins == b'\x00':
                try:  # 尝试分配任务
                    task = update()
                    surplus = str(len(urls))
                    task = list(task)
                    task.append(surplus)
                    clisock.send(json.dumps(task).encode('utf-8'))
                    print('surplus：', surplus)
                except Exception as e:  # 发生错误，把拿出来的塞回去
                    urls[task[0]] = task[1]
                    print(str(e))

            # --------------------
            # print('指令二执行')
            # --------------------
            elif ins == b'\x00\x00':
                clisock.send(b'\x02')  # 确认状态

                bname = clisock.recv(BUFSIZ)
                name = bname.decode('utf-8')
                clisock.send(b'\x02')  # 确认状态

                try:  # 开始传输文件
                    jf = open(name + '.json', 'ab')
                    while True:
                        data = clisock.recv(BUFSIZ)
                        if data == b'EOF':
                            break
                        jf.write(data)
                    jf.close()
                    print('成功获取 {0}.json 文件, 剩余 {1} 份期刊未分配'.format(name, len(urls)))

                    clisock.send(b'\x02')  # 确认状态
                    fin = clisock.recv(BUFSIZ)

                    if fin == b'\x03\x03':  # 发送确认数据，并询问是否有failed urls
                        ff = open('failed.txt', 'ab')
                        while True:
                            data = clisock.recv(BUFSIZ)
                            if data == b'EOF':
                                break
                            ff.write(data)
                        ff.close()

                except Exception as e:  # 发生错误，删除不完整的文件
                    clisock.close()
                    if os.path.exists(name + '.json'):
                        os.remove(name + '.json')
                    if os.path.exists('failed.txt'):
                        os.remove('failed.txt')
                    print(str(e))

            # --------------------
            # 未识别指令
            # --------------------
            else:
                print('错误')

            break

        # 流程最后关闭连接
        try:
            clisock.close()
        except Exception as e:
            print(str(e))
        finally:
            pass


def main():
    try:
        service()
    except Exception as ce:
        print(str(ce))
        main()


if __name__ == '__main__':
    i = input('ins(1 for new, 2 for re): ')
    if i == '1':
        with open('tasks.json', 'r', encoding='utf-8') as f:
            urls = json.loads(f.read())

        main()

    elif i == '2':
        with open('nowtasks.json', 'r', encoding='utf-8') as f:
            urls = json.loads(f.read())

        main()
