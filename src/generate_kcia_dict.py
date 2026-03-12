'''
아호코라식 알고리즘 수행에 사용할 성분 사전 생성

{
    표준 명칭: 표준 명칭,
    구명칭: 표준 명칭, 
    공백이 제거된 명칭: 표준 명칭, ...
}
'''

import pandas as pd
import json
import re


# 1. 데이터 로드
df = pd.read_csv('kcia_ingredient_dict2.csv')

# 2. 매핑 딕셔너리 초기화
kcia_mapping_dict = {}

def add_to_dict(name, std_name):
    if pd.isna(name) or str(name).strip() == 'nan' or str(name).strip() == '':
        return
    
    name = str(name).strip()
    masked_name = name.replace(",", "_C_")

    # [조건 1] 원형 이름 매핑 (구명칭/표준명칭 모두 std_name으로 귀결)
    kcia_mapping_dict[masked_name] = std_name
    
    # [조건 2] 공백이 제거된 이름 매핑
    name_no_space = masked_name.replace(" ", "")
    kcia_mapping_dict[name_no_space] = std_name

# 3. 데이터프레임 순회하며 사전 구축
for _, row in df.iterrows():
    if pd.isna(row['std_name_ko']):
        continue

    std_name = str(row['std_name_ko']).strip()
    
    # 표준 성분명 등록
    add_to_dict(std_name, std_name)
    
    # 구 성분명 등록
    add_to_dict(row['old_name_ko2'], std_name)

# 4. JSON 파일로 저장
with open('kcia_mapping_dict.json', 'w', encoding='utf-8') as f:
    json.dump(kcia_mapping_dict, f, ensure_ascii=False, indent=2)

print(f"딕셔너리 구축 완료: 총 {len(kcia_mapping_dict)}개의 키워드가 등록되었습니다.")