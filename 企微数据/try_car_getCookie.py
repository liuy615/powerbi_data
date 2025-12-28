from http.cookies import SimpleCookie
import time
from DrissionPage import ChromiumPage, ChromiumOptions,SessionPage
import os
import random
import requests

url = 'https://doc.weixin.qq.com/sheet/e3_AWwAvwZsAFAWxpI6moKS06nSeq9HC?scode=ACAAjAd5AHAfAcluL0AWwAvwZsAFA&version=4.1.36.6011&platform=win&tab=BB08J2'
co = ChromiumOptions()
co.headless(False)  # 无头模式
page = ChromiumPage(co)
print(page.address)
page.get(url)
# 等待页面加载完成
time.sleep(random.uniform(5, 10))  # 根据实际情况调整等待时间
# time.sleep(60)
ele = page.ele('xpath: //*[@id="__nuxt"]/div/div/div/div[2]/a')
if ele:
        ele.click('js')
        time.sleep(1)
# 等待页面加载完成
time.sleep(random.uniform(5, 10))  # 根据实际情况调整等待时间

# 执行 JavaScript 获取 Cookies
cookies = page.run_js('return document.cookie;')
wedoc = cookies.split(';')
wedoc_sid = ''
for i in wedoc:
    i = i.replace(' ', '')
    if 'wedoc_sid' == i.split('=')[0]:
            wedoc_sid = i.split('=')[1]
# print(cookies)

            
#以下的def download_task()是在浏览器中右键点击检查→network再按步骤点击导出excel，
# network会更新，找到export右键→copy→copy as curl(bash)，再让ai转为yhton的请求代码
# 请求下载的 task
def download_task():

    url = f'https://doc.weixin.qq.com/v1/export/export_office?sid={wedoc_sid}&wedoc_xsrf=1'

    headers = {
        'accept': '*/*',
        'accept-language': 'zh-CN,zh;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://doc.weixin.qq.com',
        'priority': 'u=1, i',
        'referer': 'https://doc.weixin.qq.com/sheet/e3_AWwAvwZsAFAWxpI6moKS06nSeq9HC?scode=ACAAjAd5AHA1FAZDahAWwAvwZsAFA&version=4.1.38.6011&platform=win&tab=BB08J2',
        'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'Cookie': cookies
    }

    data = {
        'docId': 'e3_AWwAvwZsAFAWxpI6moKS06nSeq9HC',
        'version': '2'
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        print(response.text)
        operationId = response.json()['operationId']
        # 生成时间戳
        timestamp = time.time()
        return operationId, timestamp
    
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        return None, None
    except Exception as err:
        print(f'Other error occurred: {err}')
        return None, None



# 获取下载文件的地址
def get_download_url():

    id, timestamp = download_task()

    while True:
        if id and timestamp:

            # 请求的 URL
            url = f'https://doc.weixin.qq.com/v1/export/query_progress?operationId={id}&timestamp={timestamp}'

            # 设置请求头
            headers = {
                'accept': '*/*',
                'accept-language': 'zh-CN,zh;q=0.9',
                'content-type': 'application/x-www-form-urlencoded',
                'priority': 'u=1, i',
                'referer': 'https://doc.weixin.qq.com/sheet/e3_AWwAvwZsAFAWxpI6moKS06nSeq9HC?scode=ACAAjAd5AHA1FAZDahAWwAvwZsAFA&version=4.1.38.6011&platform=win&tab=BB08J2',
                'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                # 将 cookie 转换为字符串添加到请求头
                'Cookie': cookies
            }

            try:
                # 发送 GET 请求
                response = requests.get(url, headers=headers)
                # 检查响应状态码
                response.raise_for_status()
                # 打印响应内容
                print(response.text)
                file_url = response.json().get('file_url', None)
                if file_url:
                    return file_url
                
                time.sleep(random.uniform(3, 5))
                continue

            except requests.exceptions.HTTPError as http_err:
                print(f'HTTP 错误发生: {http_err}')
            except Exception as err:
                print(f'其他错误发生: {err}')
        else:
            print('获取下载链接失败')
            return None


# 拿到连接 下载文件
def download_file():
    os.remove(r'/看板数据/dashboard/各银行授信额度统计表.xlsx')
    file_url = get_download_url()
    if file_url:
        page.download(file_url, save_path=r'/看板数据/dashboard')
        print('下载完成')
    else:
        return None
    
download_file()
page.close()