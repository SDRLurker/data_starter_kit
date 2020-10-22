## 2주차 과제

### MAU(Monthly Active User) 구하기
* [mau_count.sql](mau_count.sql)
```SQL
SELECT
	DATE_TRUNC('month', ts) AS month,
	COUNT(DISTINCT userid)
FROM raw_data.session_timestamp st, raw_data.user_session_channel usc
WHERE st.sessionid = usc.sessionid
GROUP BY month
ORDER BY month;
```
* 참고주소
  - https://stackoverflow.com/questions/17492167/group-query-results-by-month-and-year-in-postgresql

### 데이터웨어하우스에 넣을 데이터 모델링
* 중앙선거관리위원회
  * raw_data : 최근 후보자, 최근 당선자, 과거 후보자, 과거 당선자
  * analytics : 후보자 출마회수 추가, 당선자 당선회수 추가
