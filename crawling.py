import time
import json
import pymysql
import pandas as pd
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# ### 설정값 ###
MARIADB_CONFIG = {'host': '192.168.0.221', 'port': 3306, 'user': 'jongro', 'password': 'pass123#', 'db': 'jongro', 'charset': 'utf8'}
MARIADB_TABLE = 'RESTAURANTS_GENERAL'
MARIADB_COLUMN = 'STORE_NAME'
MARIADB_ADDRESS_COLUMN = 'DETAIL_ADD'
MARIADB_STATUS_COLUMN = 'OP_STATUS'

MONGO_CONFIG = {'host': '192.168.0.222', 'port': 27017, 'username': 'kevin', 'password': 'pass123#', 'db_name': 'jongro'}
MONGO_COLLECTION = 'restaurant'

NUM_PROCESSES = 2

def get_restaurant_list_from_mariadb():
    """MariaDB에서 '영업' 중인 음식점의 이름과 주소를 가져오는 함수"""
    try:
        conn = pymysql.connect(**MARIADB_CONFIG)
        query = f"SELECT `{MARIADB_COLUMN}`, `{MARIADB_ADDRESS_COLUMN}` FROM `{MARIADB_TABLE}` WHERE `{MARIADB_STATUS_COLUMN}` LIKE '%영업%'"
        df = pd.read_sql_query(query, conn)
        conn.close()
        print(f"✅ MariaDB에서 '영업' 중인 {len(df)}개의 작업 목록을 성공적으로 가져왔습니다.")
        return df.to_dict('records')
    except Exception as e:
        print(f"❌ MariaDB 연결 또는 쿼리 실패: {e}")
        return []

def get_mongodb_collection():
    """원격 MongoDB에 연결하고 컬렉션 객체와 클라이언트 객체를 반환하는 함수"""
    client = None
    try:
        if MONGO_CONFIG['username'] and MONGO_CONFIG['password']:
            uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        else:
            client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'], serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[MONGO_CONFIG['db_name']]
        return db[MONGO_COLLECTION], client
    except Exception as e:
        print(f"❌ MongoDB 연결 실패: {e}")
        if client: client.close()
        return None, None

def get_already_crawled_list():
    """MongoDB에서 이미 수집된 음식점 목록을 가져오는 함수"""
    collection, client = get_mongodb_collection()
    # *** 여기가 수정된 부분 ***
    if collection is None:
        return set()
    
    try:
        crawled_names = set(doc['original_name'] for doc in collection.find({}, {'original_name': 1}) if doc.get('original_name'))
        print(f"✅ MongoDB에서 {len(crawled_names)}개의 기수집 목록을 성공적으로 가져왔습니다.")
        return crawled_names
    except Exception as e:
        print(f"❌ MongoDB 데이터 조회 실패: {e}")
        return set()
    finally:
        if client: client.close()

def parse_apollo_data(apollo_data):
    """apollo_state 딕셔너리에서 최종 정보를 추출하는 함수"""
    try:
        data = {}
        base_key = next((key for key in apollo_data if key.startswith('PlaceDetailBase:')), None)
        if not base_key: raise ValueError("'PlaceDetailBase' 키를 찾을 수 없습니다.")
        base_data = apollo_data[base_key]
        data['name'] = base_data.get('name', '이름 정보 없음')
        visitor_reviews_str = base_data.get('visitorReviewsTotal', '0')
        data['visitor_reviews'] = int(str(visitor_reviews_str).replace(',', '')) if visitor_reviews_str else 0
        data['rating'] = base_data.get('visitorReviewsScore', 0.0)

        blog_reviews_str = '0'
        root_query = apollo_data.get('ROOT_QUERY', {})
        fsas_key = next((key for key in root_query if key.startswith('fsasReviews({')), None)
        if fsas_key:
            fsas_data = root_query[fsas_key]
            blog_reviews_str = fsas_data.get('total', '0')
        data['blog_reviews'] = int(str(blog_reviews_str).replace(',', '')) if blog_reviews_str else 0

        menu_list = []
        for key, value in apollo_data.items():
            if isinstance(value, dict) and value.get('__typename') == 'Menu':
                if value.get('name') and value.get('price'):
                    menu_list.append({'item': value.get('name'),'price': value.get('price')})
        data['menus'] = menu_list
        
        prices = [int(str(menu.get('price', '0')).replace(',', '')) for menu in menu_list if str(menu.get('price', '0')).isdigit()]
        main_menu_prices = [p for p in prices if 5000 <= p <= 80000]
        if main_menu_prices: data['avg_price'] = round(sum(main_menu_prices) / len(main_menu_prices), 2)
        else: data['avg_price'] = 0.0
        return data
    except Exception as e:
        print(f"   > [WARN] JSON 파싱 중 에러: {e}")
        return None

def worker_crawl(restaurant_info):
    """(단일 작업) 음식점 하나를 크롤링하고 MongoDB에 저장하는 함수 (주소 교차 검증 최종본)"""
    restaurant_name = restaurant_info[MARIADB_COLUMN]
    restaurant_address = restaurant_info.get(MARIADB_ADDRESS_COLUMN)
    
    driver = None
    mongo_client = None
    cleaned_name = restaurant_name.strip(' "')

    SEARCH_RESULT_LIST_SELECTOR = "#_pcmap_list_scroll_container > ul > li"
    SEARCH_RESULT_ADDRESS_SELECTOR = "span.CTXwV"
    SEARCH_RESULT_LINK_SELECTOR = "a.place_bluelink"

    try:
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.37.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
        driver = webdriver.Chrome(service=service, options=options)
        
        collection, mongo_client = get_mongodb_collection()
        if collection is None: raise Exception("MongoDB 컬렉션을 가져올 수 없습니다.")

        search_url = f"https://map.naver.com/p/search/{cleaned_name}"
        driver.get(search_url)

        try:
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))
            
            dong_info = None
            if restaurant_address:
                address_parts = restaurant_address.split()
                for part in address_parts:
                    if part.endswith('동') or part.endswith('가'):
                        dong_info = part
                        break

            target_to_click = None
            if dong_info:
                search_results = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, SEARCH_RESULT_LIST_SELECTOR)))[:5]
                for result in search_results:
                    try:
                        naver_address = result.find_element(By.CSS_SELECTOR, SEARCH_RESULT_ADDRESS_SELECTOR).text
                        if dong_info in naver_address:
                            target_to_click = result.find_element(By.CSS_SELECTOR, SEARCH_RESULT_LINK_SELECTOR)
                            break
                    except NoSuchElementException:
                        continue
            
            if target_to_click:
                target_to_click.click()
            else:
                first_result = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"{SEARCH_RESULT_LIST_SELECTOR}:first-child {SEARCH_RESULT_LINK_SELECTOR}")))
                first_result.click()

            time.sleep(2)
            driver.switch_to.default_content()
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
        except TimeoutException:
            return {'original_name': restaurant_name, 'name': cleaned_name, 'status': 'fail', 'reason': '페이지 탐색/클릭 실패'}

        apollo_state = driver.execute_script("return window.__APOLLO_STATE__;")
        parsed_data = parse_apollo_data(apollo_state)

        if parsed_data:
            parsed_data['original_name'] = restaurant_name
            parsed_data['status'] = 'success'
            collection.update_one({'original_name': restaurant_name}, {'$set': parsed_data}, upsert=True)
            return parsed_data
        else:
            return {'original_name': restaurant_name, 'name': cleaned_name, 'status': 'fail', 'reason': 'JSON 파싱 실패'}

    except Exception as e:
        return {'original_name': restaurant_name, 'name': cleaned_name, 'status': 'error', 'reason': str(e)}
    
    finally:
        if driver: driver.quit()
        if mongo_client: mongo_client.close()

if __name__ == "__main__":
    total_list_info = get_restaurant_list_from_mariadb()
    if not total_list_info:
        print("MariaDB에서 작업 목록을 가져오지 못했습니다. 스크립트를 종료합니다.")
    else:
        done_list_originals = get_already_crawled_list()
        tasks = [info for info in total_list_info if info[MARIADB_COLUMN] not in done_list_originals]
        
        if not tasks:
            print("모든 음식점 데이터가 이미 수집되었습니다. 작업을 종료합니다.")
        else:
            print(f"총 {len(total_list_info)}개 중, 이미 수집된 {len(done_list_originals)}개를 제외하고 {len(tasks)}개에 대한 크롤링을 시작합니다.")
            
            results = []
            with Pool(processes=NUM_PROCESSES) as pool:
                with tqdm(total=len(tasks), desc="Crawling Progress") as pbar:
                    for result in pool.imap_unordered(worker_crawl, tasks):
                        if result and result.get('status') == 'success':
                            pbar.set_description(f"Crawling Progress (Last: {result.get('name')})")
                        results.append(result)
                        pbar.update(1)

            print("\n--- 모든 병렬 크롤링 작업 완료 ---")
            
            success_count = sum(1 for r in results if r and r.get('status') == 'success')
            fail_count = len(results) - success_count
            
            print(f"성공: {success_count}건, 실패: {fail_count}건")