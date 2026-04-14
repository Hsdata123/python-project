import os
import re
import pytesseract
import pandas as pd
from PIL import Image
import pdfplumber
from openpyxl import load_workbook


class InvoiceProcessor:
    def __init__(self, excel_path="发票金额汇总.xlsx"):
        """初始化处理器"""
        self.excel_path = excel_path
        # 确保Excel文件存在
        if not os.path.exists(self.excel_path):
            pd.DataFrame(columns=["发票路径", "提取金额"]).to_excel(self.excel_path, index=False)

    def extract_text_from_pdf(self, pdf_path):
        """从PDF提取文本（支持多页）"""
        try:
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            return text
        except Exception as e:
            print(f"PDF处理错误（{pdf_path}）：{str(e)}")
            return ""

    def extract_text_from_image(self, image_path):
        """从图片提取文本（OCR）"""
        try:
            # 图片预处理：转为灰度图提高识别率
            img = Image.open(image_path).convert('L')
            # 识别中英文（需确保Tesseract已安装中文语言包）
            text = pytesseract.image_to_string(img, lang='chi_sim+eng')
            return text
        except Exception as e:
            print(f"图片处理错误（{image_path}）：{str(e)}")
            return ""

    def extract_amount(self, text):
        """从文本中提取金额（优先识别总金额）"""
        if not text:
            return []
        
        # 增强版正则：匹配带关键词的总金额（如"合计"、"总计"、"总金额"后的数字）
        total_pattern = r'(合计|总计|总金额)\D*¥?￥?\s*(\d{1,3}(,\d{3})*(\.\d{1,2})?|\d+\.\d{1,2})'
        total_matches = re.findall(total_pattern, text, re.IGNORECASE)
        
        if total_matches:
            # 提取总金额
            amounts = []
            for match in total_matches:
                num_str = match[1].replace(',', '')  # 去除千分位逗号
                try:
                    amounts.append(float(num_str))
                except ValueError:
                    continue
            return amounts if amounts else self._extract_general_amount(text)
        else:
            # 无关键词时提取所有可能的金额
            return self._extract_general_amount(text)

    def _extract_general_amount(self, text):
        """提取所有可能的金额（备用方法）"""
        pattern = r'¥?￥?\s*(\d{1,3}(,\d{3})*(\.\d{1,2})?|\d+\.\d{1,2})'
        matches = re.findall(pattern, text)
        amounts = []
        for match in matches:
            num_str = match[0].replace(',', '')
            try:
                amount = float(num_str)
                # 过滤不合理的小额（可根据业务调整）
                if amount > 0:
                    amounts.append(amount)
            except ValueError:
                continue
        return list(set(amounts))  # 去重

    def write_to_excel(self, file_path, amounts):
        """将结果写入Excel"""
        try:
            # 读取现有数据
            df = pd.read_excel(self.excel_path)
            # 准备新数据（每个金额对应一行）
            new_data = [{
                "发票路径": file_path,
                "提取金额": amount
            } for amount in amounts]
            # 追加数据
            new_df = pd.DataFrame(new_data)
            df = pd.concat([df, new_df], ignore_index=True)
            # 去重（避免重复处理同一文件）
            df.drop_duplicates(subset=["发票路径", "提取金额"], inplace=True)
            # 保存
            df.to_excel(self.excel_path, index=False)
            print(f"已成功写入 {len(amounts)} 个金额到Excel")
        except Exception as e:
            print(f"Excel写入错误：{str(e)}")

    def process_single_file(self, file_path):
        """处理单个发票文件"""
        print(f"\n处理文件：{file_path}")
        
        # 判断文件类型并提取文本
        if file_path.lower().endswith('.pdf'):
            text = self.extract_text_from_pdf(file_path)
        elif file_path.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            text = self.extract_text_from_image(file_path)
        else:
            print("不支持的文件格式，跳过处理")
            return
        
        # 提取金额
        amounts = self.extract_amount(text)
        if not amounts:
            print("未提取到有效金额")
            return
        
        # 显示提取结果
        print(f"提取到的金额：{[f'¥{a:.2f}' for a in amounts]}")
        
        # 写入Excel
        self.write_to_excel(file_path, amounts)

    def process_folder(self, folder_path):
        """批量处理文件夹中的所有发票"""
        if not os.path.isdir(folder_path):
            print(f"文件夹不存在：{folder_path}")
            return
        
        # 遍历文件夹中的所有文件
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                self.process_single_file(file_path)


if __name__ == "__main__":
    # 配置（根据实际情况修改）
    EXCEL_PATH = "发票金额汇总.xlsx"  # 结果Excel路径
    TARGET_PATH = "./invoices"  # 发票文件夹路径（可替换为单个文件路径）

    # 初始化处理器
    processor = InvoiceProcessor(excel_path=EXCEL_PATH)

    # 处理目标（文件夹或单个文件）
    if os.path.isdir(TARGET_PATH):
        print(f"开始批量处理文件夹：{TARGET_PATH}")
        processor.process_folder(TARGET_PATH)
    elif os.path.isfile(TARGET_PATH):
        processor.process_single_file(TARGET_PATH)
    else:
        print(f"路径不存在：{TARGET_PATH}")

    print("\n处理完成！结果已保存至：", os.path.abspath(EXCEL_PATH))