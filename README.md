# GIỚI THIỆU:
- Thu thập dữ liệu TICK của thị trường Chứng khoán Việt Nam
- URL thu thập dữ liệu: https://trading.kisvn.vn/rest/api/v2/market/symbol/{var}/quote
- VN30 bao gồm: ACB, BID, BVH, CTG, FPT, GAS, GVR, HDB, HPG, KDH, MBB, MSN, MWG, NVL, PDR, PLX, PNJ, POW, SAB, SSI, STB, TCB, TPB, VCB, VHM, VIC, VJC, VNM, VPB, VRE

```text
    VN30 thay đổi theo thời gian. 
    13/02/2023, VN30 gồm các mã trên NHƯNG thay PNJ, KDH bằng BCM, VIB 
```

***
# CÀI ĐẶT

```shell
./setup.sh
```

- Copy dữ liệu MẪU từ thư mục "sample" vào thư mục "data"
- Build docker image (Không dùng Python Virtual Environment để dễ deploy Server khác, ví dụ Server có timezone khác)
- Run docker image bằng docker-compose

***
# CHƯƠNG TRÌNH

#### Jump into Docker env
```shell
docker exec -it pi_associates_syduc bash

python3 answer.py -h
# usage: answer.py [-h] --process-name {scrape,raw,missing,liquid,liquid-for-missing} [--process-dates PROCESS_DATES]
#                  [--process-symbols PROCESS_SYMBOLS] [--log-level {INFO,DEBUG,WARNING,ERROR}] [--log-dir LOG_DIR]
#                  [--raw-data-dir RAW_DATA_DIR] [--processed-data-dir PROCESSED_DATA_DIR]
# 
# Pi-Associates answers
# 
# options:
#   -h, --help            show this help message and exit
#   --process-name {scrape,raw,missing,liquid,liquid-for-missing}
#                         Process task
#   --process-dates PROCESS_DATES
#                         Process date(s) in format %d-%m-%Y, split by comma. Default is today
#   --process-symbols PROCESS_SYMBOLS
#                         Process stock symbol(s), split by comma
#   --log-level {INFO,DEBUG,WARNING,ERROR}
#                         Log level
#   --log-dir LOG_DIR     Log directory
#   --raw-data-dir RAW_DATA_DIR
#                         Raw tick data directory
#   --processed-data-dir PROCESSED_DATA_DIR
#                         Processed tick data directory
```

### 1. Thu thập dữ liệu tick realtime
#### Thu thập dữ liệu trong ngày, thực hiện:
```shell
python3 answer.py --process-name scrape
```

#### Các module chính
| STT | Module           | Description                                                                                                                                                                                                                               |
|-----|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | scraper_runner   | Trigger "scraper_executor" dựa theo system-time <br> (9h15 đến 11h30, 13h đến 14h30, 14h45)                                                                                                                                               |
| 2   | scraper_executor | Gọi HTTP Request để lấy dữ liệu về <br> - Dummy: trả về tất cả kết quả từ HTTP Response <br> - Cache: trả về kết quả mới (KHÔNG bao gồm kết quả của lần trả về trước)                                                                     |
| 3   | delay_strategy   | Delay giữa 2 lần gọi scraper_executor <br> - Fixed: thời gian delay là cố định <br> - Dynamic: thời gian delay tùy vào số kết quả scraper_executor trả về (Ví dụ: nếu có hơn 80 kết quả mới thì lập tức scrape tiếp để tránh mất dữ liệu) |
| 4   | tick_storage     | Lưu dữ liệu Tick raw và processed sử dụng file CSV <br> Ví dụ: giao dịch NVL ngày 17/03/2023 lưu tại <DATA_DIR>/17032023/NVL.csv                                                                                                          |

```text
Lưu tick bằng file CSV làm persistent vì: 
- Dữ liệu raw, tỉ lệ lỗi cao
- Nên partition dữ liệu theo ngày giao dịch (dữ liệu 2 ngày liên tiếp nhau thì có ý nghĩa khác nhau, ví dụ: "ch" là giá thay đổi so với giá refPrice và refPrice giá khác nhau trong 2 ngày)
- Chưa có mục đích nào phức tạp hơn 
```

### Kết quả:
| Ngày       | Mô tả                                |
|------------|--------------------------------------|
| 17/03/2023 | missing nhưng không nhiều            |
| 16/03/2023 | missing từ 10h30-11h30 (server down) |

***
### 2. Xử lí dữ liệu
#### a. Tìm ra trường total volume
Ứng với mỗi Tick:
- COLUMN(mv) là KLGD của tick tương ứng ở thời điểm T
- COLUMN(vo) là KLGD tích lũy ở thời điểm T, NHƯNG chưa bao gồm giá trị COLUMN(mv) của ở thời điểm T
- COLUMN(total_volume) là KLGD tích lũy ở thời điểm T

```text
    total_volume = vo + mv
```

***
#### b. Xử lí dữ liệu Raw
#### Command
```shell
python3 answer.py --process-name raw --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level $PROCESS_LOGLEVEL
```

##### MUST run for later analyze
```shell
PROCESS_DATES="16-03-2023,17-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
PROCESS_LOGLEVEL="INFO"
python3 answer.py --process-name raw --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level $PROCESS_LOGLEVEL
```

#### Example

```shell 
# Success run
PROCESS_DATES="17-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
PROCESS_LOGLEVEL="INFO"
python3 answer.py --process-name raw --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level $PROCESS_LOGLEVEL

# Failed run, scraper in 13/03/2023 does not scrape BCM 
PROCESS_DATES="13-03-2023,14-03-2023,15-03-2023,16-03-2023,17-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
PROCESS_LOGLEVEL="INFO"
python3 answer.py --process-name raw --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level $PROCESS_LOGLEVEL

# Success run with details
PROCESS_DATES="16-03-2023"
PROCESS_LOGLEVEL="DEBUG" # Change log-level to get more detail
PROCESS_SYMBOLS="SSI"
python3 answer.py --process-name raw --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level $PROCESS_LOGLEVEL
```

#### Log Output
```
# Fast check for process "missing" run at 2023Mar18_092830
grep -e ERR -e WARN /data/kisvn/log/missing_2023Mar18_092830.log
```

#### c. Tìm ra những thời điểm bị mất tick
- Tạo cột total_volume = vo + mv
- So sánh KLGD ở ROW(N) với ROW(N-1) và ROW(N+1)
- Tick không mất dữ liệu là:
  - CURR_ROW(total_volume) = NEXT_ROW(vo)
  - CURR_ROW(vo) = PREV_ROW(vo) + PREV_ROW(mv)

#### Command
```shell
# miss a little
PROCESS_DATES="17-03-2023"
PROCESS_LOGLEVEL="DEBUG" # Change log-level to get more detail
PROCESS_SYMBOLS="VRE"
python3 answer.py --process-name missing --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS

# miss a lot example
PROCESS_DATES="16-03-2023"
PROCESS_LOGLEVEL="INFO"
PROCESS_SYMBOLS="TCB"
python3 answer.py --process-name missing --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS
```

### 3. Thanh khoản của VN30 mỗi giờ
### Cách 1:

```text
  foreach STOCK:
      group_by TRADE_HOUR:
      - AGG_MIN(vo)
      - AGG_MAX(total_volume = vo + mv)
      LIQUIDITY_PER_STOCK_PER_HOUR = AGG_MAX(vo+mv) - AGG_MIN(vo)
  
  LIQUIDITY_PER_HOUR = SUM(LIQUIDITY_PER_STOCK_PER_HOUR foreach STOCK)
``` 
#### Command
```shell
PROCESS_DATES="16-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS

PROCESS_DATES="17-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS

PROCESS_DATES="17-03-2023,16-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level INFO
```

Nhận xét: 
- Nhanh hơn cách 2 nhưng dễ sai số nếu mất nhiều dữ liệu lúc chuyển giờ
- Dữ liệu VN30 tổng của ngày 16/03/2023 chỉ có 137.9M thay vì 149M

### Cách 2:
```text
    foreach STOCK:
        create COLUMN(estimate_trade_volumne)

        if CURR_ROW.missing_tick: 
            CURR_ROW(estimate_trade_volumne) = NEXT_ROW(vo) - CURR_ROW(vo)
        else:
            CURR_ROW(estimate_trade_volumne) = CURR_ROW(mv)
        liquidity_per_stock_per_hour = AGG_SUM(estimate_trade_volume)
  
 
    LIQUIDITY_PER_HOUR = SUM(liquidity_per_stock_per_hour foreach STOCK)
```

#### Command
```shell
PROCESS_DATES="16-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid-for-missing  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS

PROCESS_DATES="17-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid-for-missing  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS

PROCESS_DATES="17-03-2023,16-03-2023"
PROCESS_SYMBOLS="ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
python3 answer.py --process-name liquid-for-missing  --process-dates $PROCESS_DATES --process-symbols $PROCESS_SYMBOLS --log-level INFO
```

***
# CLEAN UP

```shell
# normal clean
docker-compose down --volumes

# deep clean
rm -rf data
docker rmi pi_associates:syduc
```
