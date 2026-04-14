import requests
url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=33f937e5-e539-4bcb-a570-86c4463d2760'
def send_wx_markdown_v2_message(url,mentioned_list,content):
    
    data = {
        "msgtype": "markdown_v2",
        "text": {
            "content": "不用搞其他乱七八糟的东西，用这个就行，我创建群或者你创建群，然后添加消息机器人，给我那个hook地址就行",
            "mentioned_list":['sanfulai']
        }
    }

    response = requests.post(url, json=data)
    print(response.status_code)
    print(response.text)