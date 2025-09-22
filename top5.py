import pandas as pd
from pymongo import MongoClient
from collections import Counter
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# 1. MongoDB에서 데이터 가져오기 (이전과 동일)
# ---------------------------------------------------------

MONGO_CONFIG = {
    'host': '192.168.0.222', # 오타 수정: 192.168.0.222
    'port': 27017,
    'username': 'kevin',
    'password': 'pass123#',
    'db_name': 'jongro'
}
COLLECTION_NAME = 'top5'

try:
    uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
    client = MongoClient(uri)
    db = client[MONGO_CONFIG['db_name']]
    collection = db[COLLECTION_NAME]
    data_from_db = list(collection.find({}))
    client.close()
    df = pd.DataFrame(data_from_db)
    print(f"✅ MongoDB '{COLLECTION_NAME}' 컬렉션에서 {len(df)}개의 문서를 성공적으로 불러왔습니다.")
except Exception as e:
    print(f"❌ MongoDB 연결 또는 데이터 로딩 중 오류 발생: {e}")
    df = pd.DataFrame()

# ---------------------------------------------------------
# 2. 키워드 분석 및 시각화 (🔥 최종 수정된 부분)
# ---------------------------------------------------------
# ---------------------------------------------------------
# ✨ 심화 분석: 키워드 '언급 횟수'를 가중치로 적용한 분석
# ---------------------------------------------------------
if not df.empty and 'voted_keywords' in df.columns:
    # Counter 객체를 사용하여 키워드별 count를 바로 합산합니다.
    weighted_keyword_counts = Counter()
    
    for keywords_list in df['voted_keywords'].dropna():
        if isinstance(keywords_list, list):
            for keyword_dict in keywords_list:
                if isinstance(keyword_dict, dict) and 'keyword' in keyword_dict and 'count' in keyword_dict:
                    keyword = keyword_dict['keyword']
                    count = keyword_dict['count']
                    weighted_keyword_counts[keyword] += count # 기존 값에 count를 더함
    
    # 가장 많이 언급된 상위 20개 키워드 추출
    top_20_keywords = weighted_keyword_counts.most_common(20)

    # 결과 출력 및 시각화
    top_20_df = pd.DataFrame(top_20_keywords, columns=['Keyword', 'Total_Votes'])
    print("\n--- [심화 분석] 상위 20개 키워드 총 득표수 ---")
    print(top_20_df)
    
    plt.rcParams['font.family'] = 'Malgun Gothic'
    plt.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(12, 8))
    plt.barh(top_20_df['Keyword'], top_20_df['Total_Votes'], color='coral')
    plt.title('업태별 TOP 5 매장 핵심 키워드 (총 득표수 기준 상위 20개)', fontsize=16)
    plt.xlabel('총 득표수', fontsize=12)
    plt.gca().invert_yaxis()
    plt.show()