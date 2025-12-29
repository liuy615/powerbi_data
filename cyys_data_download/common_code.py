from typing import Optional, Union, Dict, List, Tuple, Any
from curl_cffi import requests
from datetime import date
import mimetypes
import logging
import time
import json
import os


class WriteLog:
    """
    日志类
    示例：
    logger = WriteLog()
    message = "这是一个示例"
    logger.log_info(message)
    logger.log_debug(message)
    logger.log_warning(message)
    logger.log_error(message)
    logger.log_critical(message)
    """
    _configured = False  # 确保单次配置

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        if not WriteLog._configured:
            # 配置日志器（允许所有级别通过）
            self.logger.setLevel(logging.DEBUG)

            # 创建日志目录
            log_dir = r'/powerbi_data\data\logs'
            os.makedirs(log_dir, exist_ok=True)

            # 文件处理器（记录所有级别）
            log_file = os.path.join(log_dir, f'{date.today()}.log')
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setLevel(logging.DEBUG)  # 文件记录所有级别

            # 控制台处理器（调整为DEBUG级别）
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)  # 关键修改点：从 WARNING 改为 DEBUG

            # 统一格式
            formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            # 添加处理器
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

            WriteLog._configured = True

    # 日志方法（支持动态消息）
    def log_info(self, message, *args):
        self.logger.info(message, *args, stacklevel=2)

    def log_debug(self, message, *args):
        self.logger.debug(message, *args, stacklevel=2)

    def log_warning(self, message, *args):
        self.logger.warning(message, *args, stacklevel=2)

    def log_error(self, message, *args):
        self.logger.error(message, *args, stacklevel=2)

    def log_critical(self, message, *args):
        self.logger.critical(message, *args, stacklevel=2)


def collect_nested_keys(data, key_list):
    """最终优化版（包含短路判断）"""
    result = {k: [] for k in key_list}
    stack = [data]

    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in result:
                    result[k].append(v)
                stack.append(v)
        elif isinstance(obj, list):
            stack.extend(reversed(obj))

    def _optimize_value(values):
        if len(values) == 1:
            return values[0]
        first = values[0]
        for item in values[1:]:
            if item != first:
                return values
        return first

    return {k: _optimize_value(v) for k, v in result.items() if v}


# 支持的浏览器类型：
# "chrome89", "chrome90", "chrome91", "chrome100", "chrome101", "chrome110",
# "edge93", "edge101", "firefox92", "firefox102", "safari15_3", "safari15_6_1", "safari16"
class RequestFunction:
    def __init__(self):
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        self.logger = WriteLog()
        self.default_timeout = 30
        self.default_impersonate = 'chrome110'
        # 初始化MIME类型数据库
        mimetypes.init()
        # 敏感关键词列表 - 修改为精确匹配
        self.sensitive_keys = {
            'password', 'passwd', 'secret', 'token',
            'key', 'credential', 'auth', 'api_key',
            'access_key', 'private_key'
        }

    def request(self, url: str, method: str = 'GET', headers: Optional[Dict] = None,
                data: Optional[Union[Dict, bytes, str]] = None,
                json_data: Optional[Dict] = None,  # 重命名json参数避免冲突
                params: Optional[Dict] = None,
                timeout: Optional[int] = None,
                impersonate: Optional[str] = None,
                verify: bool = False,
                allow_redirects: bool = True,
                cookies: Optional[Dict] = None,
                proxy: Optional[str] = None,
                stream: bool = False) -> Dict:  # 添加stream参数支持流式响应
        """
        增强版通用请求方法

        :param url: 请求地址(必填)
        :param method: 请求方法(GET/POST/PUT/DELETE等)，默认GET
        :param headers: 请求头(自动合并默认头)
        :param data: 表单格式请求体(字符串、字节或字典)
        :param json_data: JSON格式请求体(重命名为json_data避免与json模块冲突)
        :param params: URL查询参数
        :param timeout: 超时时间(秒)，默认30秒
        :param impersonate: 浏览器指纹版本，默认chrome110
        :param verify: 是否验证SSL证书，默认False
        :param allow_redirects: 是否允许重定向，默认True
        :param cookies: 请求Cookies
        :param proxy: 代理地址，格式为 "http://user:pass@host:port"
        :param stream: 是否使用流式响应，默认False
        :return: 包含请求结果的字典
        """
        # 设置默认值
        timeout = timeout or self.default_timeout
        impersonate = impersonate or self.default_impersonate

        # 合并请求头
        final_headers = {**self.default_headers, **(headers or {})}

        # 记录请求信息
        # self.logger.log_info(f"发起请求: {method} {url}")

        # 安全日志记录 - 避免记录敏感数据
        safe_log_data = self._sanitize_data(data) if data else None
        safe_log_json = self._sanitize_data(json_data) if json_data else None

        # self.logger.log_debug(f"请求参数: headers={final_headers}, params={params}, data={safe_log_data}, json_data={safe_log_json}, timeout={timeout}, impersonate={impersonate}, verify={verify}, proxy={proxy}, stream={stream}")

        try:
            # 准备请求参数
            request_args = {
                "method": method.upper(),
                "url": url,
                "headers": final_headers,
                "params": params,
                "timeout": timeout,
                "impersonate": impersonate,
                "verify": verify,
                "allow_redirects": allow_redirects,
                "cookies": cookies,
                "proxy": proxy,
                "stream": stream  # 添加stream参数
            }

            # 处理请求体
            if data is not None:
                if isinstance(data, (dict, list)):
                    # 如果是字典或列表，自动转换为JSON字符串
                    request_args["data"] = json.dumps(data)
                    if 'Content-Type' not in final_headers:
                        final_headers['Content-Type'] = 'application/json'
                else:
                    request_args["data"] = data

            if json_data is not None:
                # 使用json_data参数
                request_args["json"] = json_data
                if 'Content-Type' not in final_headers:
                    final_headers['Content-Type'] = 'application/json'

            # 发送请求
            response = requests.request(**request_args)

            # 如果是流式响应，直接返回响应对象
            if stream:
                self.logger.log_info(f"流式请求成功: {url} [状态码: {response.status_code}]")
                return {
                    'success': True,
                    'status_code': response.status_code,
                    'content_type': response.headers.get('Content-Type', '').lower(),
                    'stream': response,  # 返回原始响应对象
                    'headers': dict(response.headers),
                    'cookies': dict(response.cookies),
                    'url': response.url,
                    'elapsed': response.elapsed
                }

            # 检查HTTP状态码
            response.raise_for_status()

            # 处理响应内容
            content_type = response.headers.get('Content-Type', '').lower()
            result = self._process_response(response, content_type)

            # 记录成功日志
            # self.logger.log_info(f"请求成功: {url} [状态码: {response.status_code}]")
            # self.logger.log_debug(f"响应头: {dict(response.headers)}")

            # 构建响应字典
            response_dict = {
                'success': True,
                'status_code': response.status_code,
                'content_type': content_type,
                'data': result,
                'headers': dict(response.headers),
                'cookies': dict(response.cookies),
                'is_binary': isinstance(result, bytes),
                'url': response.url,  # 最终URL（考虑重定向）
                'elapsed': response.elapsed  # 请求耗时
            }

            # 如果数据是二进制，记录大小而不是内容
            if response_dict['is_binary']:
                self.logger.log_debug(f"接收二进制数据: {len(result)} 字节")
            else:
                # 安全记录响应内容
                safe_content = self._sanitize_data(str(result))
                # self.logger.log_debug(f"响应内容: {safe_content[:200]}{'...' if len(safe_content) > 200 else ''}")
            # self.logger.log_debug(f"请求耗时: {response_dict['elapsed']} 秒")
            return response_dict

        except requests.RequestsError as e:
            status_code = getattr(e, 'code', 500)
            error_type = type(e).__name__
            error_msg = f"请求失败: {url} - [{error_type}] {str(e)} [状态码: {status_code}]"
            self.logger.log_error(error_msg)
            return {
                'success': False,
                'error_code': status_code,
                'error': str(e),
                'error_type': error_type,
                'url': url
            }

        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"未知错误: {url} - [{error_type}] {str(e)}"
            self.logger.log_critical(error_msg)
            return {
                'success': False,
                'error_code': 500,
                'error': str(e),
                'error_type': error_type,
                'url': url
            }

    def retry_request(self, url: str, method: str = 'GET', headers: Optional[Dict] = None,
                      data: Optional[Union[Dict, bytes, str]] = None,
                      json_data: Optional[Dict] = None,  # 使用json_data避免冲突
                      params: Optional[Dict] = None,
                      timeout: Optional[int] = None,
                      impersonate: Optional[str] = None,
                      verify: bool = False,
                      allow_redirects: bool = True,
                      cookies: Optional[Dict] = None,
                      proxy: Optional[str] = None,
                      stream: bool = False,
                      retry_strategy: Optional[Dict] = None) -> Dict:
        """
        带重试机制的请求方法

        :param url: 请求地址(必填)
        :param method: 请求方法(GET/POST/PUT/DELETE等)，默认GET
        :param headers: 请求头(自动合并默认头)
        :param data: 表单格式请求体(字符串、字节或字典)
        :param json_data: JSON格式请求体(重命名为json_data避免与json模块冲突)
        :param params: URL查询参数
        :param timeout: 超时时间(秒)，默认30秒
        :param impersonate: 浏览器指纹版本，默认chrome110
        :param verify: 是否验证SSL证书，默认False
        :param allow_redirects: 是否允许重定向，默认True
        :param cookies: 请求Cookies
        :param proxy: 代理地址，格式为 "http://user:pass@host:port"
        :param stream: 是否使用流式响应，默认False
        :param retry_strategy: 重试策略配置字典，包含以下可选键:
            - max_attempts: 最大尝试次数 (默认3)
            - retry_exceptions: 需要重试的异常类型元组 (默认(requests.RequestsError,))
            - retry_status_codes: 需要重试的HTTP状态码列表 (默认[429, 500, 502, 503, 504])
            - wait_strategy: 等待策略 ('fixed'固定等待或'exponential'指数等待，默认'exponential')
            - wait_time: 等待时间(秒) (固定等待策略下使用，默认1)
            - multiplier: 指数等待的乘数 (指数等待策略下使用，默认1)
            - max_wait: 最大等待时间(秒) (指数等待策略下使用，默认10)
            - backoff_factor: 指数退避因子 (默认1.5)
            - retry_condition: 自定义重试条件函数，接受响应字典，返回布尔值 (False不重试)
        :return: 包含请求结果的字典
        """
        # 设置默认重试策略
        strategy = {
            'max_attempts': 3,
            # 修改为使用异常名称字符串
            'retry_exceptions': ('HTTPError', 'ConnectionError', 'Timeout', 'SSLError', 'RequestsError'),
            'retry_status_codes': [429, 500, 502, 503, 504],
            'wait_strategy': 'exponential',  # 默认改为指数等待
            'wait_time': 1,
            'multiplier': 1,
            'max_wait': 10,
            'backoff_factor': 1.5,  # 指数退避因子
            'retry_condition': None  # 自定义重试条件函数
        }

        # 合并用户提供的重试策略
        if retry_strategy:
            # 处理用户提供的异常类型配置
            if 'retry_exceptions' in retry_strategy and retry_strategy['retry_exceptions'] is not None:
                # 将异常类转换为类名字符串
                exceptions = []
                for e in retry_strategy['retry_exceptions']:
                    if isinstance(e, type):
                        exceptions.append(e.__name__)
                    else:
                        exceptions.append(str(e))
                strategy['retry_exceptions'] = tuple(exceptions)

            # 更新其他策略参数
            for k, v in retry_strategy.items():
                if k != 'retry_exceptions' and v is not None:
                    strategy[k] = v

        # 记录重试信息
        self.logger.log_info(f"发起带重试的请求: {method} {url}")
        self.logger.log_debug(f"重试策略: {strategy}\n{'=' * 40}")

        attempt = 1
        last_result = None

        while attempt <= strategy['max_attempts']:
            # self.logger.log_debug(f"尝试次数: {attempt}")  # 改为使用日志记录
            if attempt > 1:
                # 计算等待时间
                if strategy['wait_strategy'] == 'exponential':
                    # 使用指数退避算法
                    wait_time = min(
                        strategy['wait_time'] * (strategy['backoff_factor'] ** (attempt - 1)),
                        strategy['max_wait']
                    )
                else:
                    wait_time = strategy['wait_time']

                # 记录重试信息
                self.logger.log_warning(f"将在 {wait_time:.2f} 秒后重试 ({attempt}/{strategy['max_attempts']}): {url}\n{'=' * 40}")
                time.sleep(wait_time)

            # 执行请求
            result = self.request(
                url=url,
                method=method,
                headers=headers,
                data=data,
                json_data=json_data,
                params=params,
                timeout=timeout,
                impersonate=impersonate,
                verify=verify,
                allow_redirects=allow_redirects,
                cookies=cookies,
                proxy=proxy,
                stream=stream
            )

            # 请求成功，直接返回结果
            if result['success']:
                return result

            last_result = result

            # 检查是否需要重试
            should_retry = False

            # 1. 检查自定义重试条件
            if strategy['retry_condition'] is not None and callable(strategy['retry_condition']):
                try:
                    should_retry = strategy['retry_condition'](result)
                except Exception as e:
                    self.logger.log_error(f"自定义重试条件函数执行失败: {str(e)}")
                    should_retry = False  # 函数执行失败时不重试

            # 2. 检查异常类型 - 使用异常名称字符串进行比较
            if not should_retry and 'error_type' in result and strategy['retry_exceptions']:
                error_type = result['error_type']
                # 检查异常名称是否在重试列表中
                if error_type in strategy['retry_exceptions']:
                    should_retry = True

            # 3. 检查状态码
            if not should_retry and 'error_code' in result:
                # 确保重试状态码是列表形式
                retry_codes = strategy['retry_status_codes']
                if not isinstance(retry_codes, list):
                    retry_codes = list(retry_codes)

                if result['error_code'] in retry_codes:
                    should_retry = True

            # 4. 对于流式请求，不进行重试
            if stream:
                should_retry = False

            # 不需要重试或达到最大尝试次数，直接返回结果
            if not should_retry or attempt == strategy['max_attempts']:
                return result

            attempt += 1

        # 返回最后一次的结果
        return last_result

    def _process_response(self, response, content_type: str) -> Union[dict, list, str, bytes]:
        """处理响应内容，根据内容类型返回适当格式的数据"""
        # 尝试从Content-Type中提取MIME类型
        mime_type = content_type.split(';')[0].strip() if content_type else ''

        # 二进制内容处理
        binary_types = [
            'image/', 'video/', 'audio/', 'font/', 'application/octet-stream',
            'application/pdf', 'application/zip', 'application/x-tar',
            'application/x-gzip', 'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/msword',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/x-msdownload',  # EXE文件
            'application/java-archive',  # JAR文件
            'application/vnd.android.package-archive'  # APK文件
        ]

        if any(mime_type.startswith(t) for t in binary_types):
            return response.content

        # JSON处理
        if 'json' in mime_type or mime_type.endswith('+json'):
            try:
                return response.json()
            except (ValueError, json.JSONDecodeError) as e:
                # JSON解析失败时返回原始文本
                self.logger.log_warning(f"JSON解析失败: {str(e)}，返回原始文本")
                return response.text

        # XML处理
        if 'xml' in mime_type or mime_type.endswith('+xml'):
            return response.text

        # HTML/文本处理
        return response.text

    def _sanitize_data(self, data: Any) -> Any:
        """
        安全处理日志数据，防止敏感信息泄露
        """
        if isinstance(data, dict):
            return {k: '*****' if self._is_sensitive_key(k) else self._sanitize_data(v)
                    for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_data(item) for item in data]
        elif isinstance(data, str):
            # 尝试检测JSON字符串
            if data.startswith(('{', '[')) and data.endswith(('}', ']')):
                try:
                    parsed = json.loads(data)
                    return self._sanitize_data(parsed)
                except:
                    pass

            # 截断长字符串
            if len(data) > 200:
                return data[:200] + '...' + f' (长度: {len(data)}字符)'
        return data

    def _is_sensitive_key(self, key: str) -> bool:
        """
        检查是否为敏感键 - 修改为精确匹配
        """
        # 将键转换为小写并去除特殊字符
        clean_key = ''.join(c for c in key.lower() if c.isalnum() or c == '_')

        # 检查是否包含完整的敏感词（避免部分匹配）
        for sensitive in self.sensitive_keys:
            # 精确匹配整个单词（避免误判如 "apikey_version"）
            if sensitive in clean_key.split('_'):
                return True
        return False
