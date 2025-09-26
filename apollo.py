import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

TARGET_URL = "https://map.naver.com/p/entry/place/1658029392"

def extract_apollo_state(url):
    driver = None
    try:
        print("1. Selenium WebDriver 설정 및 시작...")
        
        options = Options()
        # ✨ [수정] 봇 탐지를 우회하기 위한 옵션들 추가
        options.add_argument("--headless")
        options.add_argument("--log-level=3")
        # 일반적인 PC 윈도우 크롬 브라우저의 User-Agent로 위장
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
        # "자동화된 소프트웨어에 의해 제어되고 있습니다" 메시지 제거
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # ✨ [수정] navigator.webdriver 플래그를 숨기는 스크립트 실행
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
            """
        })
        
        print(f"2. 목표 URL로 이동: {url}")
        driver.get(url)
        
        print("3. __APOLLO_STATE__ 데이터가 로드되기를 기다리는 중...")
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return window.__APOLLO_STATE__ && Object.keys(window.__APOLLO_STATE__).length > 0;")
            )
        except TimeoutException:
            print("❌ 대기 시간(10초) 초과: __APOLLO_STATE__를 찾을 수 없습니다.")
            # 실패 시 현재 페이지의 소스 코드를 저장하여 원인 분석
            with open("debug_page_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("   -> debug_page_source.html 파일에 현재 페이지 소스를 저장했습니다.")
            return None

        print("4. __APOLLO_STATE__ 데이터 추출 시도...")
        apollo_state = driver.execute_script("return window.__APOLLO_STATE__;")
        
        if apollo_state:
            print("✅ 데이터 추출 성공!")
            return apollo_state
        else:
            print("❌ 데이터 추출 실패: __APOLLO_STATE__가 비어있습니다.")
            return None

    except Exception as e:
        print(f"오류가 발생했습니다: {e}")
        return None
    finally:
        if driver:
            print("5. WebDriver 종료...")
            driver.quit()

if __name__ == "__main__":
    apollo_data = extract_apollo_state(TARGET_URL)
    
    if apollo_data:
        pretty_json = json.dumps(apollo_data, ensure_ascii=False, indent=2)
        print("\n--- [추출된 APOLLO_STATE 데이터] ---")
        print(pretty_json)