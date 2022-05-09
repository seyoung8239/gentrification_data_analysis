import pandas as pd
from functools import reduce

# raw file path
sales_file_path = 'raw_data/서울시_우리마을가게_상권분석서비스(신_상권_추정매출)_2021년.csv'                          # 2021
resident_file_path = 'raw_data/서울시 우리마을가게 상권분석서비스(상권-생활인구).csv'                    # 2017 ~ 2021
change_indicator_file_path = 'raw_data/서울시 우리마을가게 상권분석서비스(상권-상권변화지표).csv'    # 2017 ~ 2021

# read csv files with encoding cp949
sales_file_data = pd.read_csv(sales_file_path, encoding='cp949')
resident_file_data = pd.read_csv(resident_file_path, encoding='cp949')
change_indicator_data = pd.read_csv(change_indicator_file_path, encoding='cp949')


##############################################
# 매출 데이터 전처리
##############################################
df1 = sales_file_data

# 유의미한 서비스 업종 이외의 로우 삭제
service_list = ['인테리어', '가전제품', '가구', '화초', '섬유제품', '화장품', '시계및귀금속', '안경', '가방', '신발', '일반의류', '편의점', '슈퍼마켓',
 '피부관리실', '네일숍', '미용실', '커피-음료', '분식전문점', '패스트푸드점', '제과점', '양식음식점', '일식음식점', '중식음식점', '한식음식점',
 '노래방', '호프-간이주점', '치킨전문점', '조명용품', '서적', '청과상', 'PC방', '부동산중개업']

df1 = df1[df1['서비스_업종_코드_명'].isin(service_list)]

# 컬럼 추출
column_list1 = ['기준_분기_코드', '상권_코드', '상권_코드_명', '분기당_매출_금액']
df1 = df1[column_list1]

# 상권코드별 평균 매출 추산
df1 = df1.groupby(['상권_코드', '상권_코드_명', '기준_분기_코드']).mean()


##############################################
# 상주인구 데이터 전처리 // 분기별 변화량이 측정이 안된다... -> 생활인구로 대체
##############################################
# df2 = resident_file_data

# 2021년 데이터 추출
# df2 = df2[df2['기준 년코드'] == 2021]
# print(df2)
# print(list(df2.columns.values.tolist()))

#
# # 칼럼 추출
# column_list2 = ['기준_분기_코드', '상권 코드', '총_인구_수']
# df2 = df2[column_list2]
#
# # 컬럼 이름 변경
# df2.rename(columns={'상권 코드': '상권_코드'}, inplace=True)
#
# # ?? 분기별 인구 변화량 없음
# df2 = df2.sort_values(by=['상권_코드', '기준_분기_코드'])
# print(df2)

##############################################
# 생활인구 데이터 전처리
##############################################
df2 = resident_file_data

# 2021년 데이터 추출
df2 = df2[df2['기준 년코드'] == 2021]

# 칼럼 추출
column_list2 = ['기준_분기_코드', '상권_코드', '총_생활인구_수']
df2 = df2[column_list2]


##############################################
# 상권변화지표 데이터 전처리
##############################################
df3 = change_indicator_data

# 2021년 데이터 추출
df3 = df3[df3['기준_년_코드'] == 2021]

# 칼럼 추출
column_list3 = ['기준_분기_코드', '상권_코드', '상권_변화_지표', '상권_변화_지표_명', '운영_영업_개월_평균', '폐업_영업_개월_평균']
df3 = df3[column_list3]


##############################################
# 데이터 merge, sort
##############################################
dfs = [df1, df2, df3]

# 데이터 merge
df = reduce(lambda left, right: pd.merge(left, right, on=['상권_코드', '기준_분기_코드'], how='outer'), dfs)

# 데이터 sorting
df = df.sort_values(by=['상권_코드', '기준_분기_코드'])

# 결측치[232/6524] 제거
df = df.dropna(how='any')
# print(df['분기당_매출_금액'].isnull().sum().sum())

# 데이터 타입 변환
df['분기당_매출_금액'] = df['분기당_매출_금액'].astype(int)

# to_csv
print(df)
df.to_csv('preprocessed_data.cvs')
