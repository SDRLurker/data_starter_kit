SELECT 
	DATE_TRUNC('month', ts) AS month, 
	COUNT(DISTINCT userid) 
FROM raw_data.session_timestamp st, raw_data.user_session_channel sc 
WHERE st.sessionid = sc.sessionid 
GROUP BY month 
ORDER BY month;