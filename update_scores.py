# update_scores.py

from pymongo import MongoClient, UpdateOne
import pandas as pd

# --- ⚙️ 설정 정보 ---
MONGO_CONFIG = {
    'host': '192.168.0.222',
    'port': 27017,
    'username': 'kevin',
    'password': 'pass123#',
    'db_name': 'jongro'
}
RESTAURANTS_COLLECTION = 'RESTAURANTS_GENERAL'

def calculate_and_update_weighted_scores():
    """전체 식당의 가중 점수를 계산하고, 그 결과를 DB에 다시 업데이트하는 함수"""
    
    client = None
    try:
        print("--- MongoDB에 연결합니다... ---")
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['db_name']]
        collection = db[RESTAURANTS_COLLECTION]
        print("✅ MongoDB 연결 성공!")

        print("\n--- 1단계: 전체 맛집 데이터 불러오기 시작 ---")
        data = list(collection.find(
            {},
            # _id는 업데이트 시 필요하므로 반드시 가져와야 합니다.
            {'_id': 1, 'rating': 1, 'visitor_reviews': 1}
        ))
        if not data:
            print("❌ 오류: RESTAURANTS_GENERAL 컬렉션에 데이터가 없습니다.")
            return

        df = pd.DataFrame(data)
        print(f"✅ {len(df)}개의 맛집 데이터를 불러왔습니다.")

        print("\n--- 2단계: 가중 점수 계산 시작 ---")
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
        df['visitor_reviews'] = pd.to_numeric(df['visitor_reviews'], errors='coerce').fillna(0)
        
        C = df['rating'].mean()
        m = 200 # 최소 리뷰 수 기준점
        
        def calculate_weighted_score(row):
            v = row['visitor_reviews']
            R = row['rating']
            return (v / (v + m)) * R + (m / (v + m)) * C
            
        df['weighted_score'] = df.apply(calculate_weighted_score, axis=1)
        print("✅ 가중 점수 계산 완료.")

        print("\n--- 3단계: 계산된 점수를 DB에 업데이트 시작 ---")
        updates = []
        for index, row in df.iterrows():
            updates.append(
                UpdateOne({'_id': row['_id']}, {'$set': {'weighted_score': row['weighted_score']}})
            )
        
        if updates:
            result = collection.bulk_write(updates)
            print(f"✅ 3단계 완료: {result.modified_count}개 맛집의 가중 점수 DB 업데이트 성공!")
        else:
            print("⚠️ 업데이트할 내용이 없습니다.")

    except Exception as e:
        print(f"❌ 프로세스 중 오류 발생: {e}")
    finally:
        if client:
            client.close()
            print("\n--- MongoDB 연결을 닫습니다. ---")

if __name__ == "__main__":
    calculate_and_update_weighted_scores()
    print("프로그램을 종료합니다.")