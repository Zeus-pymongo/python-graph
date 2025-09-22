import urllib.request
import json
import re
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
import pprint
from bs4 import BeautifulSoup

# --- ⚙️ 설정 정보 ---
MONGO_CONFIG = {
    'host': '192.168.0.222',
    'port': 27017,
    'username': 'kevin',
    'password': 'pass123#',
    'db_name': 'jongro'
}
RESTAURANTS_COLLECTION = 'RESTAURANTS_GENERAL'
CRAWLED_COLLECTION = 'crawled_nave_blogs'
NAVER_CLIENT_ID = "46_7kjfK4xilqSfTXXK8"
NAVER_CLIENT_SECRET = "ZmiT61_9du"
BLOGS_PER_RESTAURANT = 10

def get_dong_top5_from_mongodb(db):
    print("--- 1단계: TOP 5 맛집 추출 시작 ---")
    collection = db[RESTAURANTS_COLLECTION]
    pipeline = [
        {'$match': {'admin_dong': {'$exists': True, '$ne': '분류불가'}}},
        {'$sort': {'weighted_score': -1}},
        {'$group': {
            '_id': '$admin_dong',
            # 🔥 변경점 1: name과 함께 category도 가져오도록 $push 수정
            'restaurants': {'$push': {'name': '$name', 'category': '$category'}}
        }},
        {'$project': {'dong': '$_id', 'top5_restaurants': {'$slice': ['$restaurants', 5]}, '_id': 0}}
    ]
    target_list = list(collection.aggregate(pipeline))
    print(f"✅ 1단계 완료: 총 {len(target_list)}개 동의 맛집 정보를 추출했습니다.")
    return target_list

def crawl_and_save_blogs_incrementally(dong_top5_list, db):
    print("\n--- 2단계: Naver API + Selenium 크롤링 (즉시 저장 방식) 시작 ---")
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        print("❌ 오류: Naver API Key를 입력해야 합니다.")
        return

    crawled_collection = db[CRAWLED_COLLECTION]
    crawled_collection.delete_many({})
    print(f"✅ '{CRAWLED_COLLECTION}' 컬렉션을 초기화했습니다.")
    
    total_saved_count = 0
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--log-level=3")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        for dong_data in dong_top5_list:
            dong_name = dong_data['dong']
            for restaurant in dong_data['top5_restaurants']:
                restaurant_name = restaurant['name']
                # 🔥 변경점 2: restaurant 딕셔너리에서 category 정보도 가져옴
                restaurant_category = restaurant['category']
                
                query = f"{dong_name} {restaurant_name}"
                encText = urllib.parse.quote(query)
                print(f"\n🔍 '{query}' 검색 시작...")
                
                # ... (API 호출 로직은 이전과 동일) ...
                api_url = f"https://openapi.naver.com/v1/search/blog?query={encText}&display={BLOGS_PER_RESTAURANT}&sort=sim"
                request = urllib.request.Request(api_url)
                request.add_header("X-Naver-Client-Id", NAVER_CLIENT_ID)
                request.add_header("X-Naver-Client-Secret", NAVER_CLIENT_SECRET)
                blog_post_info = []
                try:
                    response = urllib.request.urlopen(request)
                    rescode = response.getcode()
                    if rescode == 200:
                        response_body = response.read()
                        data = json.loads(response_body.decode('utf-8'))['items']
                        for item in data:
                            if 'blog.naver' in item['link']:
                                title = re.sub('<[^>]*>', '', item['title'])
                                blog_post_info.append({'link': item['link'], 'title': title, 'postdate': item['postdate']})
                        print(f"  ✅ API 호출 성공: {len(blog_post_info)}개의 블로그 목록 확보.")
                    else:
                        print(f"  ❌ API 오류 발생: Error Code {rescode}")
                        continue
                except Exception as e:
                    print(f"  ❗️ API 요청 중 오류: {e}")
                    continue

                for i, post in enumerate(blog_post_info):
                    print(f"  - [{i+1}/{len(blog_post_info)}] 블로그 방문 및 수집 시작...")
                    time.sleep(random.uniform(1.2, 1.8))
                    try:
                        driver.get(post['link'])
                        if driver.find_elements(By.ID, "mainFrame"):
                            driver.switch_to.frame(driver.find_element(By.ID, "mainFrame"))
                        
                        source = driver.page_source
                        html = BeautifulSoup(source, "lxml")
                        content = None
                        if html.select_one("div.se-main-container"):
                            content = html.select_one("div.se-main-container").get_text(separator='\n', strip=True)
                        elif html.select_one("div#postViewArea"):
                            content = html.select_one("div#postViewArea").get_text(separator='\n', strip=True)
                        
                        if content:
                            document = {
                                'admin_dong': dong_name, 
                                'restaurant_name': restaurant_name,
                                'category': restaurant_category, # 🔥 변경점 3: 저장할 document에 category 추가
                                'title': post['title'], 
                                'post_date': post['postdate'],
                                'blog_url': post['link'],
                                'blog_content': content
                            }
                            crawled_collection.insert_one(document)
                            total_saved_count += 1
                            print(f"    💾 저장 성공! (총 {total_saved_count}개)")
                        else:
                            print("    ⚠️ 본문을 찾지 못했습니다.")
                    except Exception as e:
                        print(f"  ❗️ 본문 수집 중 오류: {post['link']} - {e}")
                    finally:
                        driver.switch_to.default_content()
    finally:
        driver.quit()
        print("\n--- Selenium 드라이버 종료 ---")

# --- 메인 코드 실행 ---
if __name__ == "__main__":
    client = None
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['db_name']]
        target_list = get_dong_top5_from_mongodb(db)
        if target_list:
            crawl_and_save_blogs_incrementally(target_list, db)
    except Exception as e:
        print(f"❌ 전체 프로세스 중 오류 발생: {e}")
    finally:
        if client:
            client.close()
        print("프로그램을 종료합니다.")