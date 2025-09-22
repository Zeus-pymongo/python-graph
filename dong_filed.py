
import pandas as pd
from pymongo import MongoClient

# --- 1단계에서 정의한 매핑 딕셔너리 ---
# 사용자가 제공한 14개 동 기준의 매핑 딕셔너리
JONGNO_ADMIN_DONG_MAP = {
    # 행정동: 청운효자동
    '청운동': '청운효자동', '신교동': '청운효자동', '궁정동': '청운효자동', '효자동': '청운효자동',
    '창성동': '청운효자동', '통인동': '청운효자동', '누상동': '청운효자동', '누하동': '청운효자동',
    '옥인동': '청운효자동', '세종로': '청운효자동',

    # 행정동: 사직동
    '통의동': '사직동', '적선동': '사직동', '체부동': '사직동', '필운동': '사직동',
    '내자동': '사직동', '사직동': '사직동', '도렴동': '사직동', '당주동': '사직동',
    '내수동': '사직동', '신문로1가': '사직동', '신문로2가': '사직동',

    # 행정동: 삼청동
    '삼청동': '삼청동', '팔판동': '삼청동', '안국동': '삼청동', '소격동': '삼청동',
    '화동': '삼청동', '사간동': '삼청동', '송현동': '삼청동',

    # 행정동: 부암동
    '부암동': '부암동', '홍지동': '부암동', '신영동': '부암동',

    # 행정동: 평창동
    '평창동': '평창동', '구기동': '평창동',

    # 행정동: 무악동
    '무악동': '무악동',

    # 행정동: 교남동
    '교남동': '교남동', '평동': '교남동', '송월동': '교남동', '홍파동': '교남동',
    '교북동': '교남동', '행촌동': '교남동',

    # 행정동: 가회동
    '가회동': '가회동', '재동': '가회동', '계동': '가회동', '원서동': '가회동',

    # 행정동: 종로1·2·3·4가동
    '청진동': '종로1·2·3·4가동', '서린동': '종로1·2·3·4가동', '수송동': '종로1·2·3·4가동',
    '중학동': '종로1·2·3·4가동', '종로1가': '종로1·2·3·4가동', '공평동': '종로1·2·3·4가동',
    '관훈동': '종로1·2·3·4가동', '견지동': '종로1·2·3·4가동', '와룡동': '종로1·2·3·4가동',
    '권농동': '종로1·2·3·4가동', '운니동': '종로1·2·3·4가동', '익선동': '종로1·2·3·4가동',
    '경운동': '종로1·2·3·4가동', '관철동': '종로1·2·3·4가동', '인사동': '종로1·2·3·4가동',
    '낙원동': '종로1·2·3·4가동', '종로2가': '종로1·2·3·4가동', '훈정동': '종로1·2·3·4가동',
    '묘동': '종로1·2·3·4가동', '봉익동': '종로1·2·3·4가동', '돈의동': '종로1·2·3·4가동',
    '장사동': '종로1·2·3·4가동', '관수동': '종로1·2·3·4가동', '종로3가': '종로1·2·3·4가동',
    '인의동': '종로1·2·3·4가동', '예지동': '종로1·2·3·4가동', '원남동': '종로1·2·3·4가동',
    '종로4가': '종로1·2·3·4가동',

    # 행정동: 종로5·6가동
    '연지동': '종로5·6가동', '효제동': '종로5·6가동', '종로5가': '종로5·6가동',
    '충신동': '종로5·6가동', '종로6가': '종로5·6가동',

    # 행정동: 이화동
    '이화동': '이화동', '연건동': '이화동', '동숭동': '이화동',

    # 행정동: 혜화동
    '혜화동': '혜화동', '명륜1가': '혜화동', '명륜2가': '혜화동', '명륜3가': '혜화동',
    '명륜4가': '혜화동',

    # 행정동: 창신동 (창신1,2,3동 통합)
    '창신동': '창신동',

    # 행정동: 숭인동 (숭인1,2동 통합)
    '숭인동': '숭인동'
}

# --- MongoDB 연결 정보 ---
MONGO_CONFIG = {
    'host': '192.168.0.222',
    'port': 27017,
    'username': 'kevin',
    'password': 'pass123#',
    'db_name': 'jongro'
}
RESTAURANTS_COLLECTION = 'RESTAURANTS_GENERAL'

def find_admin_dong(address, mapping_dict):
    """주소 문자열을 받아 해당하는 행정동 이름을 찾아 반환하는 함수 (간소화 버전)"""
    for legal_dong, admin_dong in mapping_dict.items():
        if legal_dong in address:
            return admin_dong
    return "분류불가"

# 1. MongoDB에서 데이터 불러오기
try:
    uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[MONGO_CONFIG['db_name']]
    collection = db[RESTAURANTS_COLLECTION]
    
    restaurants_data = list(collection.find({'address': {'$regex': '종로구'}}))
    df = pd.DataFrame(restaurants_data)
    print(f"✅ MongoDB에서 {len(df)}개의 종로구 식당 데이터를 불러왔습니다.")

    # 2. 매핑 함수 적용하여 'admin_dong' 컬럼 생성
    df['admin_dong'] = df['address'].apply(lambda addr: find_admin_dong(addr, JONGNO_ADMIN_DONG_MAP))

    print("\n--- [결과] 제공된 기준(14개)으로 정제된 동별 식당 개수 ---")
    print(df['admin_dong'].value_counts())

    # 3. MongoDB에 'admin_dong' 필드 업데이트
    print("\n--- MongoDB에 'admin_dong' 필드 업데이트 시작 ---")
    update_count = 0
    for index, row in df.iterrows():
        collection.update_one(
            {'_id': row['_id']},
            {'$set': {'admin_dong': row['admin_dong']}}
        )
        update_count += 1
    print(f"✅ 총 {update_count}개 문서에 'admin_dong' 필드 업데이트를 완료했습니다.")

except Exception as e:
    print(f"❌ 데이터 처리 중 오류: {e}")
finally:
    if 'client' in locals():
        client.close()