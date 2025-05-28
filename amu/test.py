# coding: utf-8
"""
同花顺概念板块全量行情数据采集（含反爬策略）
"""

import pandas as pd
import time
import random
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import logging

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ThsConceptScraper:
    def __init__(self, concept_code):
        """
        初始化采集器
        :param concept_code: 概念编码（如'300277'）
        """
        self.concept_code = concept_code
        self.base_url = f"https://q.10jqka.com.cn/gn/detail/field/199112/order/desc/page/{{page_num}}/ajax/1/code/{concept_code}"
        self.main_url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
        self.driver = self._init_webdriver()
        self.headers_list = None
        self.total_pages = 1

    def _init_webdriver(self):
        """初始化浏览器驱动"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 无头模式
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # 随机User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        # 隐藏自动化特征
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # 初始化浏览器驱动
        service = Service('/path/to/chromedriver')  # 替换为你的chromedriver路径
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 设置浏览器指纹
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                delete navigator.__proto__.webdriver;
                window.chrome = {runtime: {}};
            """
        })
        
        return driver

    def get_concept_stocks(self):
        """
        获取概念板块全量行情数据
        :return: DataFrame - 包含所有成份股行情数据
        """
        try:
            # 首次访问主页面（模拟人工访问）
            self._visit_main_page()
            
            # 获取第一页数据
            df_first = self._get_page_data(1)
            if df_first.empty:
                logger.warning("首页面数据为空，可能被反爬拦截")
                return df_first
                
            # 获取总页数
            self.total_pages = self._extract_total_pages(df_first.columns)
            logger.info(f"共发现{self.total_pages}页数据")
            
            # 收集所有页面数据
            all_data = [df_first]
            for page in range(2, self.total_pages + 1):
                logger.info(f"正在采集第{page}页...")
                df_page = self._get_page_data(page)
                if not df_page.empty:
                    all_data.append(df_page)
                time.sleep(random.uniform(2, 4))  # 随机延迟
            
            # 数据合并
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"成功采集{len(final_df)}条数据")
            return final_df
            
        except Exception as e:
            logger.error(f"采集过程发生错误: {str(e)}", exc_info=True)
            return pd.DataFrame()
        finally:
            self.driver.quit()

    def _visit_main_page(self):
        """访问主页面获取必要Cookie"""
        try:
            self.driver.get(self.main_url)
            # 等待关键元素加载
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "page_info"))
            )
            logger.info("主页面加载成功")
        except Exception as e:
            logger.warning(f"主页面加载失败，将尝试直接采集: {str(e)}")

    def _get_page_data(self, page_num):
        """获取单页数据"""
        try:
            url = self.base_url.format(page_num=page_num)
            logger.debug(f"请求地址: {url}")
            
            self.driver.get(url)
            
            # 显式等待表格加载
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "m-table"))
            )
            
            # 获取页面内容
            html = self.driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            
            # 提取表格数据
            table = soup.find("table", class_="m-table m-pager-table")
            if not table:
                logger.warning(f"第{page_num}页表格未找到")
                return pd.DataFrame()
                
            if page_num == 1:
                self.headers_list = [th.get_text(strip=True) for th in table.find_all("th")]
                
            rows = []
            for tr in table.select("tbody tr"):
                row = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(row) == len(self.headers_list):  # 数据完整性校验
                    rows.append(row)
                    
            return pd.DataFrame(rows, columns=self.headers_list)
            
        except Exception as e:
            logger.error(f"第{page_num}页采集失败: {str(e)}")
            return pd.DataFrame()

    def _extract_total_pages(self, columns):
        """提取总页数"""
        try:
            # 先尝试从DOM中提取
            page_info = self.driver.find_element(By.CSS_SELECTOR, ".page_info").text
            total_pages = int(page_info.split("/")[-1])
            return max(1, min(total_pages, 50))  # 限制最大采集页数
        except Exception as e:
            logger.warning(f"分页提取失败，使用默认页数: {str(e)}")
            return 1

    def _clean_data(self, df):
        """数据清洗"""
        try:
            # 代码清洗
            df["代码"] = df["代码"].str.replace(r'\D+', '', regex=True).str.zfill(6)
            
            # 数值型字段转换
            numeric_fields = {
                "现价": float,
                "涨跌幅(%)": lambda x: x.str.replace('%', '').astype(float),
                "成交额": lambda x: x.str.replace('亿', '').astype(float) * 1e8,
                "流通市值": lambda x: x.str.replace('亿', '').astype(float) * 1e8,
                "市盈率": lambda x: x.replace('--', '0').astype(float)
            }
            
            for col, func in numeric_fields.items():
                if col in df.columns:
                    df[col] = df[col].apply(func)
                    
            # 时间戳标记
            df["采集时间"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logger.warning(f"数据清洗失败: {str(e)}")
            
        return df

# 使用示例
# 使用示例（需替换chromedriver路径）
if __name__ == "__main__":
    # 配置参数
    concept_code = "300277"  # 示例概念代码
    output_path = f"./{concept_code}_stocks_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
    
    try:
        # 初始化采集器
        logger.info(f"启动采集器，目标概念编码：{concept_code}")
        scraper = ThsConceptScraper(concept_code)
        
        # 执行数据采集
        logger.info("开始执行数据采集任务")
        result_df = scraper.get_concept_stocks()
        
        # 数据处理
        if not result_df.empty:
            logger.info(f"数据采集完成，共{len(result_df)}条记录，开始数据清洗...")
            cleaned_df = scraper._clean_data(result_df)
            
            # 数据预览
            print("\n数据预览：")
            print(cleaned_df.head(10).to_string(index=False))
            
            # 保存到文件
            cleaned_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            logger.info(f"数据成功保存至：{output_path}")
            
            # 采集统计
            logger.info(f"采集统计 - 总页数：{scraper.total_pages}，总记录数：{len(cleaned_df)}")
        else:
            logger.warning("未采集到有效数据，请检查网络环境或尝试更换概念编码")
            
    except Exception as e:
        logger.error(f"程序异常终止: {str(e)}", exc_info=True)
    finally:
        logger.info("数据采集任务结束")