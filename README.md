# 天龍特公地

**出發點：公有土地資訊，全民都有知的權利**

## 安裝

```shell
$ npm i -g gulp
$ npm i
```

## 前置作業

+ [設定 Google API](https://github.com/dz1984/taipei-pop/wiki/設定-Google-APIs)

+ [克隆 Fusion Tables](https://github.com/dz1984/taipei-pop/wiki/克隆-Fusion-Tables)


## 開發方式

```shell
$ npm start
```

## 資料清理工具 by Java

### Dependency 
1.  `org.apache.commons.lang3` ： 將字串轉換成 unicode 。
2.  `com.opencsv` ： 讀取 csv 檔，支援 RFC 4180。(非常好用的工具，大推)
3.  `com.google.gson` ： 處理 JSON 格式的資料。
4.  `org.postgresql.util` ： 與 Postgresql 連結，以及包裝 Postgresql 所認可的 JSON 型態資料。

### 程式功能說明
`checkTaipei_pop.java` ： 檢查整理好的 `土地.csv`、`建物.csv` 檔與現行資料庫的對應是否一對一。
`landLink.java` ： 將 `土地.csv` 配合資料庫對應的 gis 資訊，組合好後寫進新的 table。
`buildingLink.java` ： 將 `建物.csv` 配合資料庫對應的 gis 資訊，組合好後寫進新的 table。

TODO : Explain how to create a new API.

## Heroku

TODO : explain how to publish.

## 授權條款

The MIT license(MIT)
