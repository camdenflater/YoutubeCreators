--QUERY TO FIND TOP 3 HIGHEST AND LOWEST PERFORMING VIDEOS BASED ON RATIO OF TOTAL VIEWS TO TOTAL HISTORICAL VIEW AVERAGE 

DROP TABLE IF EXISTS #MasterDESC
DROP TABLE IF EXISTS #MasterASC
DROP TABLE IF EXISTS #Stage

SELECT 
[ChannelName]
,[VideoTitle]
,[DateUploaded]
,[ViewCnt]
,(SELECT AVG(CONVERT(DECIMAL, ViewCnt)) AS AvgViews 
				FROM [YT].[dbo].[YoutubeCreators]
				WHERE VideoTitle NOT LIKE '%#%'
				AND ChannelName = yc.ChannelName) AS AvgViewCnt
,CONVERT(DECIMAL, ViewCnt) / (SELECT AVG(CONVERT(DECIMAL, ViewCnt)) AS AvgViews 
				FROM [YT].[dbo].[YoutubeCreators]
				WHERE VideoTitle NOT LIKE '%#%'
				AND ChannelName = yc.ChannelName) AS ViewAvgViewRatio
,[LikeCnt]
,[CommentCnt]
,(CONVERT(DECIMAL, CommentCnt) / CONVERT(DECIMAL, ViewCnt)) * 100 AS CommentViewRatio
,(CONVERT(DECIMAL, LikeCnt) / CONVERT(DECIMAL, ViewCnt)) * 100 AS LikeViewRatio
INTO #Stage
FROM [YT].[dbo].[YoutubeCreators] yc
WHERE ViewCnt > (SELECT AVG(CONVERT(INT, ViewCnt)) 
				FROM [YT].[dbo].[YoutubeCreators]
				WHERE VideoTitle NOT LIKE '%#%'
				AND ChannelName = yc.ChannelName)
AND CommentCnt <> 0 
AND LikeCnt <> 0
ORDER BY ChannelName, ViewAvgViewRatio DESC


SELECT 
ChannelName
,CONVERT(VARCHAR(500),VideoTitle) AS VideoTitle
,ViewAvgViewRatio
,ROW_NUMBER() OVER(PARTITION BY ChannelName ORDER BY ViewAvgViewRatio DESC) AS RANK
INTO #MasterDESC
FROM #Stage
GROUP BY
ChannelName
,CONVERT(VARCHAR(500),VideoTitle)
,ViewAvgViewRatio

SELECT 
ChannelName
,CONVERT(VARCHAR(500),VideoTitle) AS VideoTitle
,ViewAvgViewRatio
,ROW_NUMBER() OVER(PARTITION BY ChannelName ORDER BY ViewAvgViewRatio ASC) AS RANK
INTO #MasterASC
FROM #Stage
GROUP BY
ChannelName
,CONVERT(VARCHAR(500),VideoTitle)
,ViewAvgViewRatio;

WITH #Ranks AS (SELECT ChannelName, VideoTitle, ViewAvgViewRatio FROM #MasterDESC
WHERE RANK < 4
UNION
SELECT ChannelName, VideoTitle, ViewAvgViewRatio FROM #MasterASC
WHERE RANK < 4)

SELECT ChannelName
,VideoTitle
,ViewAvgViewRatio
,CASE 
	WHEN ROW_NUMBER() OVER(PARTITION BY ChannelName ORDER BY ViewAvgViewRatio DESC) <= 3 THEN 'VIRAL' 
	ELSE 'AVERAGE' 
	END AS ThreeBinRank
FROM #Ranks
ORDER BY ChannelName


