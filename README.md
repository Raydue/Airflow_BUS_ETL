這是一個從TDX平台上抓公車資料的程式。

·  使用API 從TDX(政府資料平台) 去追蹤實時617路線公車的定位。設置警報機制，對資料不完整的情況做Email 通報。

·  將資料接入Tableau ，並將其進行視覺化。

·  架設Docker 作為容器，並設置Airflow 進行ETL資料流的監控與管理。

·  設計BranchOperator 去對連線及資料的完整性進行判斷。於 Yaml檔案中設定 SMTP server 功能，並使用Gmail通報。

前置作業:
架設 Docker, Airflow。設定docker-composer.yaml,SMTP-server...
--------------------------------------------------------------
![image](https://github.com/Raydue/Airflow_BUS_ETL/blob/main/BUS%20diagram.PNG)
