from sanic import Sanic
from sanic.exceptions import ServerError
from sanic.response import json as sjson
# from sanic.log import logger
import configparser
import traceback
import logging
# import sys
from retrying import retry
# import asyncio
from multiprocessing import Process #
from pathos.multiprocessing import ProcessingPool as Pool#多进程
from multiprocessing import Pool as POOL #多进程
from multiprocessing.dummy import Pool as ThreadPool #多线程
import signal
# import aiohttp
import json
import requests
import time
import uuid
import kafka
from Handle_Message import Handle_Message
from Micro_Logger import deal_log
import redis
import os
import sys
import eventlet
from functools import partial
import copy
import copyreg


#定义全局变量
KAFKA_SERVER=['127.0.0.1:9092']

SERVICE_IP='127.0.0.1'
SERVICE_PORT=3000
SERVICE_META={}

HEALTHCHECK_PATH='/health'
HEALTHCHECK_ARGS={}

REGISTER_URL='http://result.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/service/register'
RETURN_URL='http://mock.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/worker'

REDIS_IP='127.0.0.1'
REDIS_PORT=6379

# 用户在初始化时给定了参数则使用用户定义的
# 没定义则从环境变量中获取
# 环境变量中没有则使用系统默认的



class Service(object):
    def __init__(self,service_type,service_name,**kwargs): 
        try:
            self.kw=kwargs
            self.app = Sanic()
            #获取调用该库所在代码的位置
            current_dir=os.path.abspath(sys.argv[0])
            xd_dir=current_dir[0:current_dir.rfind('/')+1]

            # 日志输出 将级别为warning的输出到控制台，级别为debug及以上的输出到log.txt文件中
            logger = logging.getLogger("Service")
            logger.setLevel(logging.DEBUG)
            file_handle = logging.FileHandler(xd_dir+"log.txt", mode='a')
            cmd_handle = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(message)s")
            file_handle.setFormatter(formatter)
            cmd_handle.setFormatter(formatter)
            logger.addHandler(file_handle)
            logger.addHandler(cmd_handle)
            self.logger = logger

            self.service_name=service_name
            self.service_type=service_type

            self.kafka_cluster=self.get_config(config_name='kafka_cluster',in_user_name='kafka_cluster',in_enviorment_name='kafka_cluster',in_defalut_name=KAFKA_SERVER)
            self.service_ip=self.get_config('service_ip','service_ip','service_ip',SERVICE_IP)
            self.service_port=self.get_config('service_port','service_port','service_port',SERVICE_PORT)
            self.service_meta=self.get_config('service_meta','service_meta','service_meta',SERVICE_META)
            self.healthcheck_args=self.get_config('healthcheck_args','healthcheck_args','healthcheck_args',HEALTHCHECK_ARGS)
            self.healthcheck_path=self.get_config('healthcheck_path','healthcheck_path','healthcheck_path',HEALTHCHECK_PATH)
            self.register_url=self.get_config('register_url','register_url','register_url',REGISTER_URL)
            self.return_url=self.get_config('return_url','return_url','return_url',RETURN_URL)
            self.redis_ip=self.get_config('redis_ip','redis_ip','redis_ip',REDIS_IP)
            self.redis_port=self.get_config('redis_port','redis_port','redis_port',REDIS_PORT)
            
            #构造注册函数请求体
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

            #消息处理类所需要的变量
            self.handle = Handle_Message()
            self.handle.red = redis.Redis(host=self.redis_ip,port=self.redis_port)
            self.handle.finished_url=self.return_url
            self.handle.error_message=""

            #定义数据处理的钩子函数
            self._process_deal_func=None
            self._handle_input_item=None
            self._handle_input_items=None
            
            #健康检查的钩子函数
            self._health_check = None

        except Exception:
            self.logger.info("Errors melt in the process of initing:  "+traceback.format_exc())
            raise
    
    #依次从用户配置、环境变量和系统默认配置中获取配置
    def get_config(self,config_name,in_user_name,in_enviorment_name,in_defalut_name):
        if in_user_name in self.kw:
            temp_config=self.kw[in_user_name]
            self.logger.info("Use the config of "+config_name+" in the input of user")
        elif in_enviorment_name in os.environ and len(os.environ[in_enviorment_name]) != 0:
            temp_config=os.environ[in_enviorment_name]
            self.logger.info("Use the config of "+config_name+" in enviorment")
        else:
            temp_config=in_defalut_name
            self.logger.info("Use the config of "+config_name+" in the config of default")
        return temp_config

    #使用策略处理单条输入数据
    def handle_input_item(self,strategy=None):
        def wrapper(func):
            # #将func变为可pickle的对象,多进程执行特有
            # self._process_deal_func=copyreg.constructor(func)
            # print(type(self._process_deal_func))
            #协程和线程则不需要
            self._handle_input_item = func
            # print(type(self._handle_input_item))
            self.strategy=strategy
        return wrapper
    
    #自定义策略处理输入数据
    def handle_input_items(self):
        def wrapper(func):
            self._handle_input_items = func
        return wrapper
    
    #定义健康检查的处理函数
    def health_check(self):
        def wrapper(func):
            self._health_check= func
        return wrapper


    def _retry_on_false(result):
        return result is False

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_exponential_multiplier=1000)
    def send_message(self, mes, topic):
        try:
            mesg = str(json.dumps(mes)).encode('utf-8')
            producer = kafka.KafkaProducer(
                bootstrap_servers=self.kafka_cluster)
            producer.send(topic, mesg)
            self.logger.info("Send the message to next topic successully!")
            return True
        except Exception as err:
            return False
            raise
        finally:
            producer.close()

    # 依据data_list 和 config
    def deal_data_message(self, data_list, config):
        self.logger.info("Begin to deal data_list with config")
        config_list=[config for n in range(len(data_list))]
        #执行策略有："eventlet | thread | process"
        #执行策略 先判别单个数据的处理是否存在，若存在则使用策略对单条数据处理
        #若单条数据处理不存在则使用数据集处理函数
        try:
            start_time=time.time()
            if self._handle_input_item == None:
                result_list=self._handle_input_items(data_list,config)
            elif self.strategy == "eventlet":
                #使用协程池 处理输入数据
                
                result_list=[]
                pool = eventlet.GreenPool()
                for res in pool.imap(self._handle_input_item, data_list, config_list):
                    result_list.append(res)
        
            elif self.strategy == "thread":
                #将配置参数统一设置
                part_func=partial(self._handle_input_item,config=config)
                #使用多线程来处理输入数据
                pool = ThreadPool()
                result_list = pool.map(part_func, data_list)
                pool.close()
                pool.join()

            elif self.strategy == "process":
                #使用多进程来处理数据
                #self._process_deal_func是经过处理的函数
                #part_func=partial(self._handle_input_item,config=config)
                pool=Pool()
                result_list=pool.map(self._handle_input_item,data_list,config_list)
                # pool.close()
                # pool.join()

            else:
                self.logger.info("No strategy")
                result_list=[]
                for item in data_list:
                    result_list.append(self._handle_input_item(item,config))

            end_time=time.time()
            self.logger.info("Time cost: "+str(end_time-start_time)+"s")
            self.logger.info("The result_list is:"+str(result_list))
            return result_list
        except Exception:
            self.logger.error("Some wrong while dealing the data_list:  "+traceback.format_exc())
            raise

    #  消息解读函数
    def interpretate_message(self, message, message_type, serviceid, worktype, redis_ip="127.0.0.1", redis_port=6379):
        if message_type == 0:  # to control message handle
            self.handle.handle_control_message(message)
            return
        #获取变量
        wokerid =self.service_id
        worker_type =self.service_type

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
        
        
        # self.handle.initialize(serviceid, message, redis_ip, redis_port)
        output_flag = False  # if output_flag is true, stage finished, need to out put
        try:
            if index+1 >= len(server_list):
                self.logger.info("judge is OK")
                output_flag = True
                if not next_list and not store_list:
                    self.handle.send_finished_message(wokerid, worker_type, 0, 0,
                                                      taskid, childid, "finished", self.handle.error_message+"both next and store are null")
                    return
                message['output']['depth'] = message['output']['depth'] + 1
        except Exception as e:
            # print("----------------------------"+str(self.handle.message))
            self.logger.error("something in message need")
            self.handle.error_message = self.handle.error_message + ";" + str(e)
        if message['output']['depth'] >= message['output']['max_depth']:
            self.handle.send_finished_message(wokerid, worker_type, 0, 0, taskid,
                                              childid, "finished", self.handle.error_message)
            return
        # check config, decide use redis or not
        framework_config = config.get('framework', None)
        if framework_config is None:
            redis_config = True
        else:
            redis_config = framework_config.get("redis", True)
        if redis_config is True:
            info_list = self.handle.calculate_different_set(
                set(input_list), topic + "_" + taskid)
        else:
            info_list = input_list
        if len(info_list) <= 0:
            self.send_finished_message(wokerid, worker_type, len(info_list), 0, taskid,
                                              childid, "finished", self.handle.error_message)
            return
        result_list = self.deal_data_message(
            info_list, config.get("service", {}))
        try:
            # 处于stage的最后一个阶段，需要将数据输出到数据库和next指定的下一个stage的第一个微服务中
            if output_flag is True:
                try:
                    self.handle.store(store_list,info_list, result_list)
                except Exception as e:
                    self.l_group_id.error("the db error")
                    self.handle.error_message=self.handle.error_message+";the db error"
                finished_flag = True
                for n in next_list:  # next字段有值
                    finished_flag = False
                    self.send_message(message,topic)
                if finished_flag is True:
                    self.handle.send_finished_message(wokerid, worker_type, len(info_list), len(result_list),
                                                      taskid, childid, "finished", self.handle.error_message)
                else:
                    self.handle.send_finished_message(wokerid, worker_type, len(info_list), len(result_list),
                                                      taskid, childid, "running", self.handle.error_message)
                return
            # 不是微服务的最后一个阶段，需要将数据放到data中，通过kafka传递给下一个微服务
            else:
                message["data"] = result_list
                message["output"]["current_index"] = message["output"]["current_index"] + 1
                self.send_message(message, topic)
                self.handle.send_finished_message(wokerid, worker_type, len(info_list), len(result_list),
                                                  taskid, childid, "running", self.handle.error_message)
                return
        except Exception as e:
            self.handle.error_message = self.handle.error_message + ":" + str(e)
            traceback.print_exc()

        self.handle.insert_redis(
            set(info_list), topic + "_" + taskid)

    # 对消息的完整性进行检验
    def message_check(self, message, message_type):
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
                    "Some errors occored in the message:   "+traceback.format_exc())
                return (False, "Some errors occored in checking the message")

        else:
            # 预留控制字段信息的检查
            return (False, "control type is not support now")

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_exponential_multiplier=1000)
    def send_finish_message(self, message, info):
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
        except Exception as err:
            self.logger.error("Errors occored while sending finished message")
            return False
            raise

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_exponential_multiplier=1000)
    def send_received_message(self, message):
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
                return False
        except Exception as err:
            self.logger.error(
                "Errors occored while sending received message:  "+traceback.format_exc())
            return False
            raise

    #消息获取之后完整性检查及反馈消息的处理
    def predeal_message(self,message):
        self.logger.info("Sending message back to the controller")
        if self.send_received_message(message):
            self.logger.info("Checking the received message")
            if self.message_check(message, 1)[0]:
                # 之后这边是调用侯的代码
                self.interpretate_message(
                    message, 1, self.service_id, self.service_type)
            else:
                info = self.message_check(
                    message, 1)[1]
                self.logger.warning(
                    "Errors occored while checking the message: "+info)
                # if mes_sign[i]['message_type'] == 1:
                self.send_finish_message(message, info)
        else:
            self.logger.error(
                "Parameter missed or errors melt in sending received message")

    # kafka消息获取
    def listen_message(self):
        consumer = kafka.KafkaConsumer(
                group_id=self.task_group_id, bootstrap_servers=self.kafka_cluster)
        self.logger.info("high_topic:  "+str(self.service_high_topic))
        self.logger.info("lower_topic:  "+str(self.service_lower_topic))
        try:
            while True:
                self.logger.info("Listening the high topic message")
                consumer.subscribe(topics=[self.service_high_topic])
                message = consumer.poll(timeout_ms=5, max_records=1)
                if len(message)>0:
                    for key in message.keys():
                        message = json.loads(
                            message[key][0].value.decode('utf-8'))
                    self.logger.info("the message received in high topic:"+str(message))
                    self.predeal_message(message)
                    consumer.commit()
                    continue
                consumer.subscribe(topics=[self.service_lower_topic,self.service_high_topic])
                
                while True:
                    self.logger.info("Listening the high and lower topic message")
                    message = consumer.poll(timeout_ms=5,max_records=1)
                    if len(message)==0:
                        time.sleep(0.5)
                        continue
                    for key in message.keys():
                        message = json.loads(message[key][0].value.decode('utf-8'))
                    self.logger.info("the message received in high or lower topic:"+str(message))
                    self.predeal_message(message)
                    consumer.commit()
                    break

        except Exception:
            self.logger.info("Errors occored while polling or dealing the message:  "+traceback.format_exc())
            raise

    # 同步服务注册                          之前尝试的次数               可设定的参数，调节等待长短  ms
    # 如果返回false  重试3次 retry时间间隔=2^previous_attempt_number * wait_exponential_multiplier 和 wait_exponential_max 较小值
    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_exponential_multiplier=1000)
    def resigter_service(self):

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
            # self.service_controltopic = temp['topic']['controller']
            # 控制消息每次微服务启动时都不一致保证所有微服务都能接收到消息
            # self.c_group_id = str(uuid.uuid1())
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
            return False

    # 默认的健康检查信息
    async def default_health_check(self, request):
        return sjson({
            "state": "health",
            "info": "service is healthy"
        })

    # 添加健康检查
    def add_health_check(self):
        try:
            if self._health_check != None:
                self.app.add_route(self._health_check,
                                   uri=self.healthcheck_path)
            else:
                self.logger.warning("using default health check function")
                self.app.add_route(self.default_health_check,
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

            #健康检查注册路由
            self.add_health_check()

            def run_err_call():
                self.logger.info("Errors melt in running sainc")
                #子进程及当前主进程均关闭
                #子进程和主进程属于同一进程组，获取进程组ID之后，向进程组发送kill信号
                os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)

            #注册run_err_call来处理signal.SIGTREM信号
            signal.signal(signal.SIGTERM, run_err_call)

            #运行sanic的函数
            def run_sanic():
                #单开进程池来运行sanic，单独进程没有error_callback函数
                # self.logger.info(str(self.service_ip)+str(self.service_port))
                self.p=POOL()
                self.p.apply_async(self.app.run(self.service_ip,self.service_port),args=(),error_callback=run_err_call())

            self.process=Process(target=run_sanic)
            self.process.start()
            
            # self.p.close()
            # self.p.join()
            
            # 注册服务,重试的次数最大为3次，返回true才算成功
            if not self.resigter_service():
                self.logger.error("Errors occored while registering service")
                return
    
            #监听消息
            self.listen_message()

        except Exception:
            self.logger.error(
                "Error occored while running the main process:  "+traceback.format_exc())
            raise
