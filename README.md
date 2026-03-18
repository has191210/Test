Test
====

Test

┌─────────┐
│  start  │
└────┬────┘
     │
     ▼
┌──────────────────────────┐
│  check_need_download     │  BranchPythonOperator
│  (manifest存在？gz齐全？  │  判断是否需要从S3下载
│   manifest过时？)         │
└─────┬──────────┬─────────┘
      │          │
      │ 需要下载  │ 不需要下载
      ▼          ▼
┌───────────┐  ┌───────────────┐
│ trigger   │  │ skip          │
│ _download │  │ _download     │  EmptyOperator
│           │  │               │
│ 触发      │  └───────┬───────┘
│ daily_aws │          │
│ _s3_billing│         │
│ _data     │          │
│ _downloader│         │
└─────┬─────┘          │
      │                │
      └───────┬────────┘
              │  trigger_rule: none_failed_min_one_success
              ▼
     ┌────────────────┐
     │ download_done  │  EmptyOperator (汇合点)
     └───────┬────────┘
             │
             ▼
   ┌─────────────────────┐
   │ validate_decompress │  PythonOperator
   │                     │
   │ 1. 读取 manifest    │
   │    (data_files_for  │
   │     _etl.json)      │
   │                     │
   │ 2. 按日期目录分组    │
   │    (20250301-       │
   │     20250401 等)     │
   │                     │
   │ 3. 自动模式:        │
   │    对比新旧快照      │
   │    (.manifest       │
   │     _snapshot.json) │
   │    只保留有变化的    │
   │    目录             │
   │                     │
   │    手动模式:        │
   │    按 target_start/ │
   │    end_date 过滤    │
   │                     │
   │ 4. 解压 .gz → .csv  │
   │                     │
   │ 5. 输出 batches +   │
   │    all_csv_files    │
   └─────────┬───────────┘
             │
             ▼
    ┌─────────────────┐
    │  has_new_csv    │  ShortCircuitOperator
    │                 │
    │  有CSV文件？     │──── False ──→ 跳过下游所有任务
    │  ready_for_etl? │              直接到 end
    └────────┬────────┘
             │ True
             ▼
    ┌─────────────────────────────────────┐
    │         submit_etl                  │  PythonOperator (最长12小时)
    │                                     │
    │  逐目录循环:                         │
    │  ┌─────────────────────────────┐    │
    │  │ batch 1: 20250201-20250301  │    │
    │  │  → 生成 etl_config          │    │
    │  │  → 触发 aws_cur_etl_flow    │    │
    │  │  → 轮询等待完成 (最长3小时)    │    │
    │  └──────────────┬──────────────┘    │
    │                 ▼                   │
    │  ┌─────────────────────────────┐    │
    │  │ batch 2: 20250301-20250401  │    │
    │  │  → 触发 aws_cur_etl_flow    │    │
    │  │  → 轮询等待完成             │    │
    │  └──────────────┬──────────────┘    │
    │                 ▼                   │
    │               ...                   │
    │                                     │
    │  任何 batch 失败 → 抛异常           │
    └────────────────┬────────────────────┘
                     │
                     ▼
           ┌──────────────────┐
           │     cleanup      │  PythonOperator
           │                  │
           │ 1. 保存 manifest │
           │    快照 (ETL成功  │
           │    才保存，作为   │
           │    下次diff基线)  │
           │                  │
           │ 2. 删除解压出的  │
           │    .csv 文件     │
           │    释放磁盘空间  │
           └────────┬─────────┘
                    │
                    ▼
              ┌──────────┐
              │   end    │
              └──────────┘
