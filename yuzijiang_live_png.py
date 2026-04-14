import requests
import base64
import hashlib
import os
import time
from datetime import datetime

url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=33f937e5-e539-4bcb-a570-86c4463d2760'

def get_message(text, mentioned_list=[], mentioned_mobile_list=[]):
    return {
        "msgtype": "text",
        "text": {
            "content": text,
            "mentioned_list": mentioned_list,
            "mentioned_mobile_list": mentioned_mobile_list
        }
    }

def get_image_info(file_path):
    """获取图片的Base64编码和MD5值"""
    try:
        # 读取图片文件
        with open(file_path, "rb") as image_file:
            img_data = image_file.read()
        
        # 计算MD5值
        md5_hash = hashlib.md5(img_data).hexdigest()
        
        # 转换为Base64编码
        base64_data = base64.b64encode(img_data).decode('utf-8')
        
        return {
            "msgtype": "image",
            "image": {
                "base64": base64_data,
                "md5": md5_hash
            }
        }
    except Exception as e:
        print(f"处理图片时出错: {str(e)}")
        return None

def save_image_with_timestamp(original_path, save_directory=None):
    """
    将图片添加时间戳后缀并另存为
    
    Args:
        original_path: 原始图片路径
        save_directory: 保存目录，如果为None则保存在原目录
    
    Returns:
        new_path: 新文件路径
    """
    try:
        # 获取文件目录和文件名
        file_dir, filename = os.path.split(original_path)
        
        # 如果未指定保存目录，则使用原目录
        if save_directory is None:
            save_directory = file_dir
        
        # 分离文件名和扩展名
        name, ext = os.path.splitext(filename)
        
        # 生成时间戳格式：yyyy-mm-dd HH:MM:SS
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 构建新文件名
        new_filename = f"{name}_{timestamp}{ext}"
        new_path = os.path.join(save_directory, new_filename)
        
        # 复制文件
        with open(original_path, "rb") as original_file:
            file_data = original_file.read()
        
        with open(new_path, "wb") as new_file:
            new_file.write(file_data)
        
        print(f"图片已保存为: {new_path}")
        return new_path
        
    except Exception as e:
        print(f"保存图片时出错: {str(e)}")
        return None

if __name__ == '__main__':
    path = r"D:\rpa\live_data_png"
    file_name = "鱼子酱.png"  # 添加了文件扩展名
    file_path = os.path.join(path, file_name)
    print(f"原始文件路径: {file_path}")
    
    # 等待60秒
    time.sleep(20)
    
    # 发送文本消息
    message_data = get_message(file_name.replace(".png", ""))
    response = requests.post(url, json=message_data)
    print(f"文本消息发送状态: {response.status_code}")
    print(f"文本消息响应: {response.text}")
    
    # 发送图片消息
    image_data = get_image_info(file_path)
    if image_data:
        response = requests.post(url, json=image_data)
        print(f"图片消息发送状态: {response.status_code}")
        print(f"图片消息响应: {response.text}")
        
        # 如果发送成功，保存带时间戳的图片
        if response.status_code == 200:
            save_image_with_timestamp(file_path)
    else:
        print("图片处理失败，无法发送")