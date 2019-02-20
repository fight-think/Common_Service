# 测试程序的执行
from service import Service
from sanic.response import json
import time

sv = Service("DNS", "DNS_test")


def FBNQ(num):
    if num == 1:
        return 1
    elif num == 2:
        return 1
    else:
        a = 1
        b = 1
        while num > 2:
            c = a
            a = b
            b = c+b
            num = num-1
        return b


# 调用系统的运行策略  该函数以协程、多线程或者多进程的方式运
# 传入的是单个处理数据,strategy可为 eventlet process thread，pool_size代表线程池|进程池的大小
# time_out代表处理单条数据所需要的最长等待时间，若该时间内不返回结果则报错
# 如果使用的是协程，该函数应该使用 async 定义
@sv.handle_input_item(strategy="process",  pool_size=5, time_out=2)
def handle_item(single_request_item, config):
    # 可以调用其他地方的代码或者API来处理单个输入
    # time.sleep(10)
    time.sleep(10)
    return FBNQ(single_request_item)

# 自定义运行策略，传入的处理数据集


@sv.handle_input_items(time_out=4)
def handle_items(request_items, config):
    # 在里面可以定义自己的运行策略
    time.sleep(2)
    result_list = list(map(FBNQ, request_items))
    return result_list


@sv.health_check()
def health_check(request):
    return json({
        "status": "health",
        "infor": "wwwww"
    })

# print("the process is "+str(__name__))


sv.run()
