from sanic import Sanic
from sanic.exceptions import ServerError
from sanic.response import json as sjson
# from sanic.log import logger
import configparser
import traceback
import logging
import log_config
from cloghandler import ConcurrentRotatingFileHandler
# import sys
from retrying import retry
# import eventlet
import asyncio
import uvloop
import multiprocessing
from multiprocessing import Process
from pathos.multiprocessing import ProcessingPool as Pool  # 多进程
from multiprocessing import Pool as POOL  # 多进程
from multiprocessing.dummy import Pool as ThreadPool  # 多线程
import signal
# import aiohttp
import json
import requests
import time
import uuid
import kafka
# from Handle_Message import Handle_Message
# from Micro_Logger import deal_log
import redis
import os
import sys
import eventlet
from functools import partial
import copy
import copyreg
import types

from Database_Handle import MongoDB_Store
from Database_Handle import MySql_Store
import redis

#设置事件循环策略 使得asyncio.get_event_loop() 返回一个 uvloop 实例
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# 定义全局变量
KAFKA_SERVER = ['127.0.0.1:9092']

SERVICE_IP = '127.0.0.1'
SERVICE_PORT = 3000
SERVICE_META = {}

HEALTHCHECK_PATH = '/health'
HEALTHCHECK_ARGS = {}

REGISTER_URL = 'http://result.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/service/register'
RETURN_URL = 'http://mock.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/worker'

REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379

RETRY_TIMES = 3
RETRY_TIME_UNIT = 1000

# 用户在初始化时给定了参数则使用用户定义的
# 没定义则从环境变量中获取
# 环境变量中没有则使用系统默认的

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copyreg.pickle(types.MethodType, _pickle_method)





class Service(object):
    def __init__(self, service_type, service_name, **kwargs):
        try:
            self.kw = kwargs
            self.app = Sanic()
            # 获取调用该库所在代码的位置
            current_dir = os.path.abspath(sys.argv[0])
            self.xd_dir = current_dir[0:current_dir.rfind('/')+1]

            # log_filename=xd_dir+"logtest.txt"
            # log_config.config['handlers']['file']['filename']=log_filename
            # logging.config.dictConfig(log_config.config)
            # self.logger=logging.getLogger('Service')

            # 日志输出 将级别为warning的输出到控制台，级别为debug及以上的输出到log.txt文件中
            logger = logging.getLogger('Service')
            logger.setLevel(logging.DEBUG)
            # 文件名，写入模式（a表示追加），文件大小（2M），最多保存5个文件
            # ConcurrentRotatingFileHandle能解决多进程日志的文件写入问题
            file_handle = ConcurrentRotatingFileHandler(
                self.xd_dir+"log.txt",  "a", 2*1024*1024, 5)
            cmd_handle = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")
            file_handle.setFormatter(formatter)
            cmd_handle.setFormatter(formatter)
            logger.addHandler(file_handle)
            logger.addHandler(cmd_handle)
            self.logger = logger

            # 重试的次数和单位时间间隔
            self.retry_times = 3
            self.retry_time_unit = 1000  # ms

            self.service_name = service_name
            self.service_type = service_type

            self._update_variables()
            

            # 构造注册函数请求体
            self.server_register_parameter = {
                "name": self.service_name,
                "type": self.service_type,
                "address": self.service_ip,
                "port": self.service_port,
                "meta": self.service_meta,
                "check": {
                    "args": self.healthcheck_args,
                    "path": self.healthcheck_path
                }
            }

            # 定义数据处理的钩子函数
            self._process_deal_func = None
            self._handle_input_item = None
            self._handle_input_items = None

            # 健康检查的钩子函数
            self._health_check = None

            # 保持一个redis连接
            self.redis_handle = redis.Redis(
                host=self.redis_ip, port=self.redis_port)

        except Exception:
            self.logger.info(
                "Errors occured in the process of initializing:  "+traceback.format_exc())
            raise

    def _update_variables(self):
        self.kafka_cluster = self._get_config(
            config_name='kafka_cluster', in_user_name='kafka_cluster', in_enviorment_name='kafka_cluster', in_defalut_name=KAFKA_SERVER)
        self.service_ip = self._get_config(
            'service_ip', 'service_ip', 'service_ip', SERVICE_IP)
        self.service_port = self._get_config(
            'service_port', 'service_port', 'service_port', SERVICE_PORT)
        self.service_meta = self._get_config(
            'service_meta', 'service_meta', 'service_meta', SERVICE_META)
        self.healthcheck_args = self._get_config(
            'healthcheck_args', 'healthcheck_args', 'healthcheck_args', HEALTHCHECK_ARGS)
        self.healthcheck_path = self._get_config(
            'healthcheck_path', 'healthcheck_path', 'healthcheck_path', HEALTHCHECK_PATH)
        self.register_url = self._get_config(
            'register_url', 'register_url', 'register_url', REGISTER_URL)
        self.return_url = self._get_config(
            'return_url', 'return_url', 'return_url', RETURN_URL)
        self.redis_ip = self._get_config(
            'redis_ip', 'redis_ip', 'redis_ip', REDIS_IP)
        self.redis_port = self._get_config(
            'redis_port', 'redis_port', 'redis_port', REDIS_PORT)
        
        self.logger.info("Update the variables successfully!")


    # 依次从用户配置、环境变量和系统默认配置中获取配置
    def _get_config(self, config_name, in_user_name, in_enviorment_name, in_defalut_name):
        if in_user_name in self.kw:
            temp_config = self.kw[in_user_name]
            self.logger.info("Use the config of " +
                             config_name+" in the input of user")
        elif in_enviorment_name in os.environ and len(os.environ[in_enviorment_name]):
            temp_config = os.environ[in_enviorment_name]
            self.logger.info("Use the config of "+config_name+" in enviorment")
        else:
            temp_config = in_defalut_name
            self.logger.info("Use the config of "+config_name +
                             " in the config of default")
        return temp_config

    # 使用策略处理单条输入数据
    def handle_input_item(self, strategy=None,pool_size=4,time_out=3):
        def wrapper(func):
            self._handle_input_item = func
            #获取用户对框架的配置
            self.pool_size=pool_size
            self.strategy = strategy
            self.time_out=time_out
        return wrapper

    # 自定义策略处理输入数据
    def handle_input_items(self,time_out=4):
        def wrapper(func):
            self._handle_input_items = func
            self.mult_time_out=time_out
        return wrapper

    # 定义健康检查的处理函数
    def health_check(self):
        def wrapper(func):
            self._health_check = func
        return wrapper

    def _retry_on_false(result):
        return result is False

    # , retry_on_result=_retry_on_false,
    @retry(stop_max_attempt_number=RETRY_TIMES, wait_exponential_multiplier=RETRY_TIME_UNIT)
    def _send_message(self, mes, topic):
        try:
            mesg = str(json.dumps(mes)).encode('utf-8')
            producer = kafka.KafkaProducer(
                bootstrap_servers=self.kafka_cluster)
            producer.send(topic, mesg)
            self.logger.info("Send the message to next topic successully!")
            producer.close()
        except Exception:
            self.logger.error(
                "Errors occured while sending message to next topic")
            if not producer:
                producer.close()
            raise

    # 依据data_list 和 config

    def _handle_data_message(self, data_list, config):
        self.logger.info("Begin to deal data_list with config")
        config_list = [config for n in range(len(data_list))]
        # 执行策略有："eventlet | thread | process"
        # 执行策略 先判别单个数据的处理是否存在，若存在则使用策略对单条数据处理
        # 若单条数据处理不存在则使用数据集处理函数
        # if self.strategy and self.time_out and self.pool_size:
        #     self.logger.info(str(self.strategy)+"  "+str(self.pool_size)+"   "+str(self.time_out))
        
        #对于没有设定运行策略，单条数据循环执行需要设置等待的时间
        #不使用单条执行策略，调用用户的集中执行函数也需要设置等待时间
        #如果接收到超时信号则会raise错误
        def handler(signum, frame):
            raise AssertionError

        # data_list 和 config_list 为两个参数列表
        #对于单条数据处理的控制，池中的每个协程、线程、进程 等待若干时间没有返回结果就
        result_list=[]
        try:
            start_time = time.time()
            if self._handle_input_item == None:
                try:
                    signal.signal(signal.SIGALRM, handler)
                    signal.alarm(self.mult_time_out)
                    res= self._handle_input_items(data_list, config)
                    for i in range(0,len(data_list)):
                        single_result={
                            "data":data_list[i],
                            "value":res[i],
                            "credible":True,
                            "info":''
                        }
                        result_list.append(single_result)
                    signal.alarm(0)
                except AssertionError:
                    for i in range(0,len(data_list)):
                        single_result={
                            "data":data_list[i],
                            "value":'',
                            "credible":False,
                            "info":'time_out'
                        }
                        result_list.append(single_result)
                
            elif self.strategy == "eventlet":
                # 使用协程池 处理输入数据
                # asyncio uvloop
                loop=asyncio.get_event_loop()
                tasks=[]
                for item in data_list:
                    coroutine=self._handle_input_item(item,config)
                    c_to_feature = asyncio.ensure_future(coroutine)
                    tasks.append(c_to_feature)
                loop.run_until_complete(asyncio.wait(tasks,timeout=self.time_out*len(data_list)))

                for i in range(0,len(tasks)):
                    try:
                        temp_result=tasks[i].result()
                        single_result={
                            "data":data_list[i],
                            "value":temp_result,
                            "credible":True,
                            "info":''
                        }
                        result_list.append(single_result)
                    except asyncio.InvalidStateError:
                        single_result={
                            "data":data_list[i],
                            "value":'',
                            "credible":False,
                            "info":"time_out"
                        }
                        result_list.append(single_result)

                        

                # result_list = []
                # pool = eventlet.GreenPool()
                # for res in pool.imap(self._handle_input_item, data_list, config_list):
                #     result_list.append(res)

            elif self.strategy == "thread":
                # 将配置参数统一设置
                part_func = partial(self._handle_input_item, config=config)
                # 使用多线程来处理输入数据
                pool = ThreadPool(self.pool_size)

                results=[]

                for item in data_list:
                    result=pool.apply_async(part_func,args=(item,))
                    results.append(result)
                
                for i in range(0,len(results)):
                    try:
                        res=results[i].get(timeout=self.time_out)
                        single_result={
                            "data":data_list[i],
                            "value":res,
                            "credible":True,
                            "info":''
                        }
                        result_list.append(single_result)
                    except multiprocessing.TimeoutError:
                        single_result={
                            "data":data_list[i],
                            "value":'',
                            "credible":False,
                            "info":"time_out"
                        }
                        result_list.append(single_result)

                pool.close()
                pool.join()            
                    
                # result_list = pool.map(part_func, data_list)
                # pool.close()
                # pool.join()

            elif self.strategy == "process":
                
                # 使用多进程来处理数据

                #这种方法暂时行不通，不能对单个的处理函数进行计时，只能使用map对全体执行函数计时
                # self._process_deal_func是经过处理的函数
                # part_func=partial(self._handle_input_item,config=config)
                # pool = POOL(self.pool_size)
                # # result_list = pool.apply_async(
                # #     self._handle_input_item,args=())
                
                # results=[]

                # for item in data_list:
                #     result=pool.apply_async(part_func,args=(item,))
                #     results.append(result)
                
                # for i in range(0,len(results)):
                #     try:
                #         res=results[i].get(timeout=self.time_out)
                #         single_result={
                #             "data":data_list[i],
                #             "value":res,
                #             "credible":True,
                #             "info":''
                #         }
                #         result_list.append(single_result)
                #     except multiprocessing.TimeoutError:
                #         single_result={
                #             "data":data_list[i],
                #             "value":'',
                #             "credible":False,
                #             "info":"time_out"
                #         }
                #         result_list.append(single_result)

                # pool.close()
                # pool.join()

                pool = Pool(self.pool_size)
                try:
                    signal.signal(signal.SIGALRM, handler)
                    signal.alarm(self.time_out*len(data_list))
                    res= pool.map(self._handle_input_item,data_list,config_list)
                    for i in range(0,len(data_list)):
                        single_result={
                            "data":data_list[i],
                            "value":res[i],
                            "credible":True,
                            "info":''
                        }
                        result_list.append(single_result)
                    signal.alarm(0)
                except AssertionError:
                    for i in range(0,len(data_list)):
                        single_result={
                            "data":data_list[i],
                            "value":'',
                            "credible":False,
                            "info":'time_out'
                        }
                        result_list.append(single_result)
                
                # pool.close()
                # pool.join()

            else:
                self.logger.info("No strategy")
                for i in range(0,len(data_list)):
                    try:
                        signal.signal(signal.SIGALRM, handler)
                        signal.alarm(self.time_out)
                        res=self._handle_input_item(data_list[i], config)
                        single_result={
                            "data":data_list[i],
                            "value":res,
                            "credible":True,
                            "info":''
                        }
                        result_list.append(single_result)
                        signal.alarm(0)
                    except AssertionError:
                        single_result={
                            "data":data_list[i],
                            "value":'',
                            "credible":False,
                            "info":'time_out'
                        }
                        result_list.append(single_result)

            end_time = time.time()

            self.logger.info("Time cost: "+str(end_time-start_time)+"s")   
            
            self.logger.info("The result after handling:"+str(result_list))
            
            return result_list
        
        except Exception:
            self.logger.error(
                "Some wrong while dealing the data_list:  "+traceback.format_exc())
            raise

    # 对消息的完整性进行检验
    def _message_check(self, message, message_type):
        if message_type == 1:
            try:
                if 'childid' not in message:
                    return (False, "the childid is missing")
                else:
                    childid = message.get('childid', None)
                    if type(childid) != int:
                        return (False, "childid must be int")

                if 'taskid' not in message:
                    return (False, "the taskid is missing")

                if 'data' not in message:
                    return (False, "the data missing")
                else:
                    data = message.get('data', None)
                    if type(data) != list:
                        return (False, "the data must be list")

                if 'output' not in message:
                    return (False, "the output is missing")
                else:
                    output = message.get('output', None)
                    if type(output) != dict:
                        return (False, "the output must be dict")
                    else:
                        if 'current_stage' not in output:
                            return (False, "the current_stage is missing")

                        if 'current_index' not in output:
                            return (False, "the current_index is missing")
                        else:
                            current_index = output['current_index']
                            if type(current_index) != int:
                                return (False, "the current_index must be int")

                        if 'depth' not in output:
                            return (False, "the depth is missing")
                        else:
                            depth = output['depth']
                            if type(depth) != int:
                                return (False, "the depth must be int")

                        if 'max_depth' not in output:
                            return (False, "the max_depth is missing")
                        else:
                            max_depth = output['max_depth']
                            if type(max_depth) != int:
                                return (False, "the max_depth must be int")

                        if 'stages' not in output:
                            return (False, "the stages is missing")
                        else:
                            stages = output['stages']
                            if type(stages) != dict:
                                return (False, "the stages must be dict")
                            else:
                                for key in stages.keys():
                                    if type(stages[key]) != dict:
                                        return (False, "stage in stages must be dict")
                                    else:
                                        temp = stages[key]

                                        if 'microservices' not in temp:
                                            return (False, "the microservices is missing")

                                        if 'next' not in temp:
                                            return (False, "the next is missing")

                                        if 'store' not in temp:
                                            return (False, "the store is missing")

                return (True, "the message is right")

            except Exception as err:
                self.logger.error(
                    "Some errors occured in the message:   "+traceback.format_exc())
                return (False, "Some errors occured in checking the message")

        else:
            # 预留控制字段信息的检查
            return (False, "Control type is not support now")

    @retry(stop_max_attempt_number=RETRY_TIMES, wait_exponential_multiplie=RETRY_TIME_UNIT)
    def _send_finish_message(self, message, info):
        if 'taskid' not in message:
            temp_taskid = None
        else:
            temp_taskid = message['taskid']

        if 'childid' not in message:
            temp_childid = -1
        else:
            temp_childid = message['childid']

        send_message = {
            "type": "received",
            "workerid": self.service_id,
            "worker_type": self.service_type,
            "valid_input_length": 0,
            "output_length": 0,
            "taskid": temp_taskid,
            "childid": temp_childid,
            "status": "finished",
            "error_msg": info
        }
        parametas = json.dumps(send_message)
        try:
            ret = requests.put(self.return_url, params=parametas, timeout=2)
            temp = ret.json()
            self.logger.info("after sending finished message: "+str(temp))
            if temp['state'] == 0:
                self.logger.info("task finished")
                return True
            else:
                self.logger.error(
                    "the parameters of sending finished message is wrong")
                return False
        except Exception:
            self.logger.error("Errors occored while sending finished message")
            raise

    @retry(stop_max_attempt_number=RETRY_TIMES, wait_exponential_multiplier=RETRY_TIME_UNIT)
    def _send_received_message(self, message):
        if 'taskid' not in message:
            temp_taskid = None
        else:
            temp_taskid = message['taskid']

        if 'childid' not in message:
            temp_childid = -1
        else:
            temp_childid = message['childid']

        send_message = {
            "type": "received",
            "workerid": self.service_id,
            "worker_type": self.service_type,
            "taskid": temp_taskid,
            "childid": temp_childid,
            "task_message": message
        }
        parametas = json.dumps(send_message)
        try:
            ret = requests.put(self.return_url, params=parametas, timeout=2)

            temp = ret.json()
            self.logger.info(str(temp))
            if temp['state'] == 0 and temp['status'] == "running":
                self.logger.info("The task need to be done")
                return True
            else:
                self.logger.info("Don't need to do the task")
                return False
        except Exception:
            self.logger.error(
                "Errors occored while sending received message:  "+traceback.format_exc())
            raise

    # 消息获取之后完整性检查及反馈消息的处理
    def _predeal_message(self, message):
        try:
            self.logger.info("Sending message back to the controller")
            if self._send_received_message(message):
                self.logger.info("Checking the received message")
                if self._message_check(message, 1)[0]:
                    # 之后这边是调用侯的代码
                    self._interpretate_message(
                        message, 1)
                    self.logger.info("One task has been done")
                    #更新环境变量
                    
                    self._update_variables()

                else:
                    info = self._message_check(
                        message, 1)[1]
                    self.logger.warning(
                        "Errors occored while checking the message: "+info)
                    self._send_finish_message(message, info)
            else:
                self.logger.error(
                    "Parameter missed or errors occured or task passed by controller in sending received message")      
        except Exception:
            self.logger.error("Errors occured during predealing the message")
            raise

    # kafka消息获取
    def _listen_message(self):
        consumer = kafka.KafkaConsumer(
            group_id=self.task_group_id, bootstrap_servers=self.kafka_cluster)
        self.logger.info("high_topic:  "+str(self.service_high_topic))
        self.logger.info("lower_topic:  "+str(self.service_lower_topic))
        try:
            while True:
                self.logger.info("Listening the high topic message")
                consumer.subscribe(topics=[self.service_high_topic])
                message = consumer.poll(timeout_ms=2000, max_records=1)
                if len(message) > 0:
                    for key in message.keys():
                        message = json.loads(
                            message[key][0].value.decode('utf-8'))
                    self.logger.info(
                        "the message received in high topic:"+str(message))
                    self._predeal_message(message)
                    consumer.commit()
                    continue
                consumer.subscribe(
                    topics=[self.service_lower_topic, self.service_high_topic])

                while True:
                    self.logger.info(
                        "Listening the high and lower topic message")
                    message = consumer.poll(timeout_ms=2000, max_records=1)
                    if len(message) == 0:
                        time.sleep(0.5)
                        continue
                    for key in message.keys():
                        message = json.loads(
                            message[key][0].value.decode('utf-8'))
                    self.logger.info(
                        "the message received in high or lower topic:"+str(message))
                    self._predeal_message(message)
                    consumer.commit()
                    break

        except Exception:
            self.logger.info(
                "Errors occored while polling or dealing the message:  "+traceback.format_exc())
            raise

    # 同步服务注册                          之前尝试的次数               可设定的参数，调节等待长短  ms
    # 如果返回false  重试3次 retry时间间隔=2^previous_attempt_number * wait_exponential_multiplier 和 wait_exponential_max 较小值
    @retry(stop_max_attempt_number=RETRY_TIMES, wait_exponential_multiplier=RETRY_TIME_UNIT)
    def _resigter_service(self):

        # self.logger.info("time now="+str(time.time()))
        parametas = json.dumps(self.server_register_parameter)
        try:
            # 设置的超时时间为两秒
            ret = requests.post(self.register_url, params=parametas, timeout=2)
            temp = ret.json()
            self.service_id = temp['id']
            # print(self.service_id)
            self.service_lower_topic = temp['topic']['low_priority']
            self.service_high_topic = temp['topic']['high_priority']

            self.task_group_id = "task_group"  # 高优先级group
            # self.l_group_id = "l_group"  # 低优先级group
            self.service_state = temp['state']
            # print(type(self.service_state))
            # print(self.service_state)
            if self.service_state is True:
                self.logger.info('Registered service successfully!')
                return True
            else:
                self.logger.error(
                    'Registered service unsuccessfully with the fail of service manager')
                return False
        except Exception:
            self.logger.error('Registered service unsuccessfully   ' +
                              traceback.format_exc())
            raise

    # 默认的健康检查信息
    async def _default_health_check(self, request):
        return sjson({
            "state": "health",
            "info": "service is healthy"
        })

    # 添加健康检查
    def _add_health_check(self):
        try:
            if self._health_check != None:
                self.app.add_route(self._health_check,
                                   uri=self.healthcheck_path)
            else:
                self.logger.warning("using default health check function")
                self.app.add_route(self._default_health_check,
                                   uri=self.healthcheck_path)
        except Exception:
            self.logger.error(
                "Error occored during adding healthcheck route of sanic: "+traceback.format_exc())
            raise
    # 服务运行

    def run(self):
        try:
            if self._handle_input_item == None and self._handle_input_items == None:
                self.logger.error("No handling function")
                return

            self.mpid = os.getpid()
            self.mgid=os.getpgid(self.mpid)
            self.logger.info("ID of main process is:"+str(self.mpid))
            self.logger.info("ID of group of main process is:"+str(self.mgid))
            with open('./gid.txt', 'w') as f:
                f.write(str(self.mgid))

            # 健康检查注册路由
            self._add_health_check()

            def _run_err_call(gid):
                self.spid = os.getpid()
                self.sgid=os.getpgid(self.spid)
                self.logger.info("ID of sub process is:"+str(self.spid))
                self.logger.info("ID of group of sub process is:"+str(self.sgid))
                self.logger.info("Errors melt in running sainc")
                
                # 关闭子进程和主进程
                # os.kill(self.spid,signal.SIGKILL)
                # os.kill(self.mpid,signal.SIGKILL)

                # 子进程及当前主进程均关闭
                # 子进程和主进程属于同一进程组，获取进程组ID之后，向进程组发送kill信号
                os.killpg(gid, signal.SIGKILL)


            # 运行sanic的函数
            def _run_sanic():
                # 单开进程池来运行sanic，单独进程没有error_callback函数
                self.p = POOL(2)
                self.p.apply_async(self.app.run(
                    self.service_ip, self.service_port), args=(), error_callback=_run_err_call(self.mgid))
  
            self.process = Process(target=_run_sanic)
            self.process.start()

            subpid=self.process.pid
            subgid=os.getpgid(subpid)
            self.logger.info("ID of sub process is:"+str(subpid))
            self.logger.info("ID of group of sub process is:"+str(subgid))

            # 注册服务,重试的次数最大为3次，返回true才算成功
            if not self._resigter_service():
                self.logger.error(
                    "Register service unsucessfully! Check service manager")
                return

            # 监听消息
            self._listen_message()

        except Exception:
            self.logger.error(
                "Error occored while running the main process:  "+traceback.format_exc())
            return
    
    

    # 终止服务
    def stop():
        #检查pid是否存在
        def check_pid(pid):        
            try:
                os.kill(pid, 0)
            except OSError:
                return False
            else:
                return True

        #获取到的gid即为主进程的pid,可以用检查pid的方法检查gid是否存在
        with open('./gid.txt', 'r') as f:
            gid = int(f.read())
            print("Gid in file:"+str(gid))

        if check_pid(gid):
            os.killpg(gid,signal.SIGKILL)
            print("Kill the service sucessully")
        else:
            print("No such process group")

    #  消息解读函数

    def _interpretate_message(self, message, message_type):
        # 每次处理消息，都需要将error_info字典初始化为空
        self.error_info = {}
        if message_type == 0:  # to control message handle
            self._handle_control_message(message)
            return
        # 获取变量
        stage = message['output']['current_stage']
        index = message['output']['current_index']
        next_list = message['output']['stages'][stage]['next']
        store_list = message['output']['stages'][stage]['store']
        server_list = message['output']['stages'][stage]['microservices']
        taskid = message["taskid"]
        childid = message["childid"]
        input_list = message['data']
        topic = message['output']["stages"][stage]["microservices"][index]["topic"]
        config = message['output']["stages"][stage]["microservices"][index]["config"]

        # if output_flag is true, this stage finished, need to send infomation to API
        output_flag = False
        try:
            # 条件成立，表示当前微服务是当前stage的最后一个阶段，处理完成后需要进行输出
            # 所以，需要查看next列表和store列表，以确定阶段结束后数据的流向
            if index + 1 >= len(server_list):
                output_flag = True
                # 如果next列表和store列表为空，表示数据没有输出，后续过程无意义，结束任务
                if not next_list and not store_list:
                    self.error_info["output_error"] = "the next list and store list are empty. there is no output path"
                    self._send_finished_message(
                        0, 0, taskid, childid, "finished", self.error_info)
                    return
                message['output']['depth'] = message['output']['depth'] + 1
        except Exception as e:
            self.logger.error("something in message need" +
                              traceback.format_exc())
            self.error_info["message_error"] = "something in message is needed"

        # 进行深度判断，如果当前深度大于最大深度，则结束任务
        if message['output']['depth'] >= message['output']['max_depth']:
            self._send_finished_message(
                0, 0, taskid, childid, "finished", self.error_info)
            return
        self.logger.info("start redis handle")
        # 通过配置信息，决定是否使用redis．redis存储了该微服务以往执行的历史数据
        framework_config = config.get('framework', None)
        # 框架配置为空，默认使用redis;不为空，根据用户的选择决定是否使用redis
        if framework_config is None:
            redis_config = True
        else:
            redis_config = framework_config.get("redis", True)
        # redis_config为真，则根据历史数据去重之后的数据作为真正的输入；否则，message的输入作为真的输入　　　　　　　
        if redis_config:
            info_list = self._calculate_different_set(
                set(input_list), topic + "_" + taskid)
        else:
            info_list = input_list

        # 如果真输入为空，则该微服务结束,发送结束消息
        if not info_list:
            self._send_finished_message(
                0, 0, taskid, childid, "finished", self.error_info)
            return
        # 进行数据转换
        info_list = list(map(int, info_list))
        # info_list=[1,2,3,4,5]
        # 进行数据计算
        self.logger.info("calculate data")
        result_list = self._handle_data_message(
            info_list, config.get("service", {}))

        # output_flag为真，则表示处于stage的最后一个阶段，
        # 需要将数据输出到数据库，和next指定的下一个stage的第一个微服务中
        self.logger.info("output to database")
        if output_flag:
            # 存入数据库
            try:
                self._store(store_list, info_list, result_list)
            except Exception as e:
                self.logger.error("An error occurred while storing data.")
                self.error_info["store_error"] = "An error occurred while storing data."

            self.logger.info("send finished message")
            # 根据next列表,将数据放入到下一个微服务的topic中
            finished_flag = True     # finished_flag为真，不存在着下一个阶段；　finished_flag为假，存在着下一个阶段
            for n in next_list:  # next字段有值
                finished_flag = False
                try:
                    self._send_message(message, topic)
                except Exception as e:
                    self.logger.error(
                        "An error occurred while sending message to kafka." + traceback.format_exc())
                    self.error_info["store_error"] = "An error occurred while storing data."
            # finished_flag为真,则没有next stage, 任务结束;else,任务继续执行
            if finished_flag:
                self._send_finished_message(len(info_list), len(result_list),
                                            taskid, childid, "finished", self.error_info)
            else:
                self._send_finished_message(len(info_list), len(result_list),
                                            taskid, childid, "running", self.error_info)

        # output_flag为假,则表示处于stage的最后一个阶段,需要将数据放到data中，通过kafka传递给下一个微服务
        else:
            self.logger.info("output_flag is false")
            message["data"] = result_list
            message["output"]["current_index"] = message["output"]["current_index"] + 1
            try:
                self._send_message(message, topic)
            except Exception as e:
                self.logger.error(
                    "An error occurred while sending message to kafka." + traceback.format_exc())
                self.error_info["store_error"] = "An error occurred while storing data."
            self._send_finished_message(len(info_list), len(result_list),
                                        taskid, childid, "running", self.error_info)
        self.logger.info("add data to history set")
        self._insert_redis(topic + "_" + taskid)

    # 函数功能:对控制消息进行处理
    def _handle_control_message(self, message):
        print("this is handle_control_message")

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def _send(self, m):
        resp = requests.put(self.return_url, params=json.dumps(m), timeout=3)
        response_dic = resp.json()
        return response_dic

    # send finshed message and error message to the finished_api
    def _send_finished_message(self, valid_input_length, output_length,
                               taskid, childid, status, error_info):
        m = {
            "type": "finished",
            "wokerid": self.service_id,
            "worker_type": self.service_type,
            "valid_input_length": valid_input_length,
            "output_length": output_length,
            "taskid": taskid,
            "childid": childid,
            "status": status,
            "error_msg": error_info
        }
        self.logger.info(m)
        try:
            response_dic = self._send(m)
        except Exception as e:
            err = "Error 111: the Api which received finished message can not reached"
            self.logger.error(err + traceback.format_exc())
            return
        # get response text. 0:success, -2:para is wrong
        self.logger.info(response_dic)
        if not response_dic.get("state", -2) == 0:
            info = "Error: exception occur in send_finished_message function. the url or json data is wrong"
            self.logger.error(info)
        else:
            self.logger.info("send finished message success")

    # 函数功能: 计算输入数据和历史数据的差集，并将差集返回
    # 通过retry装饰器,来控制３次重连
    # ３次重连失败，会跑出链接redis失败的信息,并将异常返回到上一级
    @retry(stop_max_attempt_number=4, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def _get_sub(self, set_name, info_set):
        try:
            self.redis_handle.delete("set_help")     # 清空辅助redis.set集合

            # 使用redis的pipeline技术,批量上传数据
            pipe = self.redis_handle.pipeline(transaction=False)
            # ་将数据存放到set_help,使用pipeline
            for value in info_set:
                pipe.sadd("set_help", value)
            pipe.execute()

            # 将set_help和历史记录做差
            self.redis_handle.sdiffstore("set_help", "set_help", set_name)
            sub = self.redis_handle.sinter("set_help")
            # 返回set_help中的数据
            return sub
        except Exception as e:
            # 重连redis
            self.redis_handle = redis.Redis(
                host=self.redis_ip, port=self.redis_port, decode_responses=True)
            pipe = self.redis_handle.pipeline(transaction=False)
            raise

    # 函数功能：将输入数据和历史数据作差集
    # 输入：set1:输入集合，set_name:集合名字，redis_?：链接信息
    # 输出：与历史记录的差集
    def _calculate_different_set(self, set1, set_name):
        r_list = list()

        # 计算差集；　如果出现异常, redis数据库连接失败,输出错误信息, 将输入数据作为真实数据输出
        try:
            r_list = list(self._get_sub(set_name, set1))
        except Exception as e:
            r_list = list(set1)
            self.logger.error(
                "Connection to the redis refused in calculate_different_set")
            self.error_info["redis_error1"] = str(e)
        finally:
            return r_list

    # 函数功能:　将真实数据插入到历史数据集合中
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def _add_info_list(self, set_name):
        # 将set1中的数据添加到历史数据的set集合中
        try:
            self.redis_handle.sunion(set_name, set_name, "set_help")
        except Exception as e:
            self.redis_handle = redis.Redis(
                host=self.redis_ip, port=self.redis_port, decode_responses=True)
            raise

    # 函数功能：将数据插入到redis中
    # 输入：set1:输入集合，set_name:集合名字，redis_?：链接信息
    # 输出：将数据插入到redis.set()中
    def _insert_redis(self, set_name):
        try:
            self._add_info_list(set_name)
        except Exception as e:
            self.logger.error(
                "Connection to the redis refused in insert_redis function")
            self.error_info["redis_error1"] = str(e)

    # 函数功能: 按照store list, 进行数据存储
    def _store(self, store_list, data_list, result_list):
        if not store_list:
            return
        for s_way in store_list:
            # self.logger.info("I am here")
            if s_way["type"] == "mongoDB":
                url = s_way["url"]
                db_name = s_way["database"]
                col_name = s_way["collection"]
                mongodb = MongoDB_Store()
                try:
                    mongodb.store_data_list(
                        url, db_name, col_name, data_list, result_list)
                except Exception as e:
                    error_msg = "store data by mongodb is wrong in store function"
                    self.logger.error(error_msg + traceback.format_exc())
                    self.error_info["strore"] = error_msg

            elif s_way["type"] == "mysql":
                i = 0
                while i < len(data_list):
                    sql = "insert into table(key,vlaue) values('{key}','{value}')".format(key=str(data_list[i]),
                                                                                          value=str(result_list[i]))
                    MySql_Store().insert(sql)
                    i = i + 1
                pass
            elif s_way["type"] == "file":
                pass
            else:
                pass
