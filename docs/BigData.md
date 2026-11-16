### Corporación Favorita Grocery Sales Forecasting là lựa chọn xuất sắc để phô diễn sức mạnh của Big Data vì dữ liệu thực sự lớn (bảng `train.csv` gốc lên tới \~125 triệu dòng, dung lượng \~5GB). Với quy mô này, việc xử lý đơn luồng trên Pandas gần như bất khả thi trên máy cá nhân, giúp bạn chứng minh rõ rệt giá trị của Apache Spark chạy trên Docker.

### 1\. Phân tích Dataset & Chiến lược chọn Subset

#### Phân tích đặc trưng 5V (Dùng trực tiếp cho Báo cáo & Slide)

* **Volume:** Bảng giao dịch chính (train.csv) chứa 125.49 triệu dòng từ năm 2013 đến 2017, kết hợp với 6 bảng phụ trợ (stores.csv, items.csv, transactions.csv, oil.csv, holidays\_events.csv). Tổng dung lượng \~5.2 GB.  
* **Variety:** Dữ liệu đa dạng cấu trúc: Chuỗi thời gian (Time-series sales), dữ liệu phân loại sản phẩm (33 nhóm hàng \- family), dữ liệu địa lý (54 cửa hàng tại 22 thành phố của Ecuador), yếu tố kinh tế vĩ mô (giá dầu thô hàng ngày oil.csv vì kinh tế Ecuador phụ thuộc vào dầu mỏ) và lịch sự kiện/ngày lễ quốc gia.  
* **Velocity:** Dữ liệu POS (Point-of-Sale) được ghi nhận theo tần suất hàng ngày (daily granularity) tại tất cả các chi nhánh trên toàn quốc.  
* **Veracity:** Có nhiều thách thức về độ tin cậy: giá trị khuyết (onpromotion bị thiếu dữ liệu trước tháng 4/2014, giá dầu bị trống vào cuối tuần), giá trị âm (hàng bị trả lại \- returns), và độ thưa dữ liệu (sparse data \- không phải ngày nào cửa hàng cũng bán hết tất cả 4,100 mã hàng).  
* **Value:** Dự báo chính xác nhu cầu tiêu thụ giúp chuỗi bán lẻ tối ưu tồn kho, giảm lãng phí thực phẩm tươi sống (perishable goods) và tránh tình trạng đứt gãy chuỗi cung ứng (stockouts).

#### Cách chọn Subset thông minh (Đảm bảo tính đại diện & vừa sức máy cá nhân)

Vì 125 triệu dòng sẽ tốn rất nhiều thời gian để chạy thử nghiệm nhiều lần trên máy cá nhân (ví dụ 16GB RAM), nhóm nên chọn một **Subset có chủ đích** theo 2 chiều (Thời gian \+ Không gian):

1. **Lọc theo thời gian:** Lấy dữ liệu từ **01/01/2016 đến 15/08/2017** (khoảng 1.5 năm gần nhất). *Lý do:* Dữ liệu giai đoạn này đầy đủ thông tin khuyến mãi (onpromotion không bị null), phản ánh sát hành vi tiêu dùng hiện đại và loại bỏ nhiễu từ các năm quá cũ.  
2. **Lọc theo cửa hàng (Stratified Store Selection):** Thay vì lấy 54 cửa hàng, chọn **10 cửa hàng đại diện** trải đều qua cả 5 phân hạng cửa hàng (cluster từ 1 đến 17, và type A, B, C, D, E) tại các thành phố lớn (Quito, Guayaquil) và thành phố nhỏ.  
* **Kết quả Subset:** Thu được khoảng **10 – 15 triệu dòng** (\~500MB \- 800MB). Con số này đủ lớn để tạo áp lực thực sự cho hệ thống (thấy rõ sự khác biệt khi đổi RAM/Worker), nhưng vẫn chạy xong 1 vòng thực nghiệm trong vài phút.

### 2\. Kiến trúc Hệ thống với Docker (Deployment Architecture)

Sử dụng **Docker Compose** để giả lập một cụm máy chủ phân tán hoàn chỉnh ngay trên máy cá nhân. Kiến trúc gồm 4 thành phần chính:

1. **Spark Master Container:** Điều phối tài nguyên và quản lý các tác vụ (DAG Scheduler). Mở port 8080 (Spark Web UI) để chụp ảnh màn hình giám sát đưa vào báo cáo.  
2. **Spark Worker Containers (Worker 1, Worker 2, Worker 3):** Các node tính toán thực thi tác vụ. Cấu hình giới hạn tài nguyên cứng bằng thuộc tính deploy.resources.limits (ví dụ mỗi worker 2 CPUs, 3GB RAM) để thực hiện thí nghiệm mở rộng tài nguyên.  
3. **JupyterLab (PySpark Driver) Container:** Môi trường viết code tương tác, kết nối trực tiếp tới spark://spark-master:7077.  
4. **Shared Volume (./data):** Thư mục dùng chung gắn vào tất cả container để lưu trữ dữ liệu gốc (CSV) và dữ liệu đã tối ưu (Parquet).

### 3\. Quy trình Xử lý Dữ liệu & Mô hình Hóa (Data Pipeline)

Bài toán được định nghĩa là **Tabular Time-Series Regression** (Dự báo doanh số unit\_sales theo ngày cho từng cặp store\_nbr \- item\_nbr).

#### Bước 1: Data Cleaning & Relational Joining (PySpark SQL)

* Xử lý giá trị âm của unit\_sales (đưa về 0 vì đó là hàng trả lại, không phải nhu cầu mua).  
* Inner/Left Join bảng train với stores, items, oil (dùng kỹ thuật Forward Fill để điền giá dầu ngày cuối tuần), và holidays\_events (chỉ lấy các ngày lễ không bị chuyển dời transferred \== False).

#### Bước 2: Feature Engineering (Tạo đặc trưng chuỗi thời gian trên Spark)

Sử dụng **Spark Window Functions** (Window.partitionBy("store\_nbr", "item\_nbr").orderBy("date")) — đây là điểm ăn tiền về kỹ thuật Big Data vì tính toán Window trên hàng chục triệu dòng đòi hỏi cơ chế Shuffle dữ liệu phân tán:

* **Calendar Features:** day\_of\_week, day\_of\_month, month, is\_weekend, is\_payday (tại Ecuador, người lao động thường nhận lương vào ngày 15 và ngày cuối tháng, làm doanh số siêu thị tăng vọt).  
* **Lag Features:** Doanh số của chính sản phẩm đó tại cửa hàng đó cách đây 7 ngày (lag\_7), 14 ngày (lag\_14), và 28 ngày (lag\_28).  
* **Rolling Statistics:** Trung bình trượt doanh số trong 7 ngày gần nhất (rolling\_mean\_7) và 28 ngày gần nhất (rolling\_mean\_28).  
* **Target Transformation:** Chuyển đổi biến mục tiêu sang dạng logarit: $y=\log (1+unit\_sales)$ để khớp với chuẩn đánh giá **NWRMSLE** (Normalized Weighted Root Mean Squared Logarithmic Error) của bài toán gốc.

#### Bước 3: Huấn luyện Mô hình (Spark MLlib)

* **Chia tập dữ liệu (Time-based Split):** Tuyệt đối không chia ngẫu nhiên (random split) vì sẽ gây rò rỉ dữ liệu tương lai (data leakage). Dùng dữ liệu đến hết **31/07/2017** làm tập Train, và **15 ngày đầu tháng 8/2017** làm tập Test.  
* **3 Mô hình so sánh:**  
  1. **Linear Regression (Ridge/Lasso):** Mô hình tuyến tính cơ sở, tốc độ huấn luyện cực nhanh.  
  2. **Random Forest Regressor:** Xử lý tốt các biến phân loại và phi tuyến tính.  
  3. **Gradient-Boosted Trees (GBTRegressor):** Mô hình mạnh nhất cho dữ liệu bảng, nhưng tốn nhiều chi phí tính toán và bộ nhớ nhất.

### 4\. Kịch bản Thực nghiệm Chi tiết (Experiments)

Nhóm cần chạy và thu thập số liệu từ **Spark Web UI (localhost:8080)** cho 4 thí nghiệm sau:

1. **Thí nghiệm 1: So sánh hiệu năng lưu trữ (Storage & I/O Benchmark)**  
   * **Cách làm:** Thực hiện cùng một truy vấn Join \+ GroupBy trên tập 15 triệu dòng lưu ở định dạng **CSV** so với định dạng **Parquet** (nén Snappy).  
   * **Đo lường:** Thời gian đọc dữ liệu (I/O Time) và dung lượng ổ đĩa chiếm dụng. *(Dự kiến: Parquet giảm \~70-80% dung lượng và đọc nhanh hơn 3-5 lần nhờ cơ chế lưu trữ theo cột và predicate pushdown).*  
2. **Thí nghiệm 2: So sánh thuật toán dự báo (Model Accuracy vs. Cost)**  
   * **Cách làm:** Huấn luyện 3 mô hình (Linear Regression, Random Forest, GBT) trên cùng tập Train.  
   * **Đo lường:** Chỉ số **RMSE / RMSLE** trên tập Test và **Training Time (giây)**. Phân tích sự đánh đổi (trade-off) giữa độ chính xác và thời gian huấn luyện.  
3. **Thí nghiệm 3: Khả năng mở rộng theo dữ liệu (Data Scalability)**  
   * **Cách làm:** Giữ nguyên cấu hình Docker (2 Workers, mỗi worker 2 Cores \- 3GB RAM). Chạy toàn bộ pipeline (ETL \+ Feature Engineering \+ Train Random Forest) với 4 mức kích thước dữ liệu tăng dần: **2 triệu $\rightarrow$ 5 triệu $\rightarrow$ 10 triệu $\rightarrow$ 15 triệu dòng**.  
   * **Đo lường:** Tổng thời gian thực thi (Execution Time) và tỷ lệ tăng trưởng thời gian có tuyến tính hay không.  
4. **Thí nghiệm 4: Khả năng mở rộng theo tài nguyên (Horizontal Resource Scaling)**  
   * **Cách làm:** Giữ nguyên 15 triệu dòng dữ liệu. Thay đổi số lượng container Worker trong Docker Compose: **1 Worker (2 cores)** $\rightarrow$ **2 Workers (4 cores)** $\rightarrow$ **3 Workers (6 cores)**.  
   * **Đo lường:** Thời gian chạy, hệ số tăng tốc (**Speedup**), và lượng dữ liệu **Shuffle Read / Shuffle Write** quan sát được trên Spark UI.

### 5\. Cấu trúc Báo cáo & Thuyết trình Tiếng Anh (15 phút)

Để tối ưu thời gian 15 phút trình bày bằng tiếng Anh, nhóm nên phân bổ slide như sau:

* **Slide 1–2 (2 phút):** Title & Problem Statement (Tầm quan trọng của Demand Forecasting trong chuỗi cung ứng bán lẻ).  
* **Slide 3–4 (2.5 phút):** Dataset Overview, 5V Analysis & Representative Subset Strategy (Giải thích lý do chọn giai đoạn 2016–2017 và 10 cửa hàng đại diện).  
* **Slide 5–6 (3 phút):** System Architecture (Sơ đồ cụm Docker \+ Spark) & Feature Engineering Pipeline (Nhấn mạnh Window Functions cho Lag/Rolling features và Payday feature).  
* **Slide 7–11 (5.5 phút \- Trọng tâm):** Experimental Results & System Analysis (Trình bày 4 biểu đồ tương ứng với 4 thí nghiệm kèm nhận xét kỹ thuật sâu sắc về I/O, Shuffle overhead và Trade-off của mô hình).  
* **Slide 12 (2 phút):** Conclusion, Limitations & Future Work (Đề xuất tích hợp Apache Kafka để nhận dữ liệu POS theo luồng thời gian thực).

### **1\. Phân chia vai trò & Trách nhiệm (3 Thành viên)**

| Thành viên | Vai trò chính (Pipeline Stage) | Nhiệm vụ chi tiết & Sản phẩm bàn giao (Deliverables) |
| :---- | :---- | :---- |
| **Thành** *(Phần đầu)* | **Data & Infrastructure Engineer** (Hạ tầng Docker \+ Tiền xử lý Dữ liệu) | • Dựng cụm Apache Spark (1 Master, 3 Workers) \+ JupyterLab bằng **Docker Compose** (có giới hạn CPU/RAM). • Tải bộ dữ liệu *Corporación Favorita*, thực hiện EDA (phân tích 5V, missing values). • Viết script PySpark lọc **Subset đại diện** (2016–2017, 10 cửa hàng), join 6 bảng dữ liệu, làm sạch và xuất ra **Parquet**. • Chạy **Thí nghiệm 1 (Storage Benchmark: CSV vs. Parquet)** và chụp lại chỉ số dung lượng/tốc độ đọc. |
| **Tiến** *(Phần giữa)* | **ML & Feature Engineer** (Đặc trưng chuỗi thời gian \+ Mô hình dự báo) | • Nhận dữ liệu Parquet sạch từ Thành, viết pipeline **Feature Engineering** bằng PySpark Window Functions (lag\_7, lag\_14, rolling\_mean\_7, is\_payday...). • Chia tập Train/Test theo mốc thời gian (Time-based split) và huấn luyện 3 mô hình trên **Spark MLlib** (Linear Regression, Random Forest, GBT). • Chạy **Thí nghiệm 2 (Model Comparison: RMSE/RMSLE vs. Training Time)** và lưu lại script chạy end-to-end cho Thiện. |
| **Thiện** *(Phần cuối)* | **System Performance, Report & Pitch Lead** (Thực nghiệm mở rộng \+ Báo cáo & Thuyết trình) | • Nhận pipeline hoàn chỉnh từ Tiến, chạy **Thí nghiệm 3 (Data Scalability: 2M $\rightarrow$ 15M dòng)** và **Thí nghiệm 4 (Resource Scaling: 1 $\rightarrow$ 2 $\rightarrow$ 3 Workers)**. • Thu thập số liệu từ Spark Web UI (Execution time, Speedup, Shuffle Read/Write) và vẽ toàn bộ biểu đồ trực quan. • Tổng hợp nội dung, viết **Báo cáo cuối kỳ bằng tiếng Anh** và thiết kế **Slide tiếng Anh (15 phút)**. |

### **2\. Lộ trình triển khai chi tiết (16/11 – 05/12/2026)**

Để tránh tình trạng người sau phải ngồi chờ người trước làm xong mới bắt đầu, nhóm cần áp dụng **giao tiếp qua file mẫu (Sample Data)**: ngay ngày thứ 3, Thành xuất một file nhỏ (khoảng 100k dòng) để Tiến code trước pipeline mô hình.

| Mốc thời gian | Thành (Hạ tầng & Data) | Tiến (Features & Models) | Thiện (Experiments & Docs) | Cột mốc đồng bộ (Sync) |
| :---- | :---- | :---- | :---- | :---- |
| **Giai đoạn 1:**  16/11 – 20/11 *(5 ngày)* | • Viết docker-compose.yml chạy ổn định cụm Spark. • Tải data Kaggle, làm sạch & xuất **Sample Parquet (100k dòng)** cho Tiến. • Hoàn thiện script xuất **Full Subset Parquet (15M dòng)**. | • Chạy thử môi trường Docker của Thành. • Nhận Sample Parquet, viết trước code Feature Engineering (lag, rolling) và khung code 3 mô hình MLlib. | • Lên khung sườn **Báo cáo tiếng Anh** (LaTeX/Word) & khung Slide. • Viết trước phần *Problem Statement* & *5V Analysis*. • Chuẩn bị script đo lường thời gian/tài nguyên. | **20/11:** Chốt hạ tầng Docker & có bộ dữ liệu Parquet sạch 15M dòng. |
| **Giai đoạn 2:**  21/11 – 25/11 *(5 ngày)* | • Chạy **Thí nghiệm 1 (CSV vs. Parquet)**, ghi nhận số liệu I/O và dung lượng. • Chụp ảnh kiến trúc cụm trên Spark UI, viết nháp phần *System & Data* gửi Thiện. | • Chạy pipeline trên **Full Subset (15M dòng)**. • Tinh chỉnh tham số và chạy **Thí nghiệm 2 (So sánh 3 mô hình)**. • Đóng gói code thành 1 file script sạch để Thiện benchmark. | • Hỗ trợ Tiến debug lỗi Out-Of-Memory (OOM) khi shuffle trên Spark. • Nhận số liệu TN1 từ Thành, bắt đầu viết phần *Architecture & Data Pipeline* bằng tiếng Anh. | **25/11:** Hoàn thành code MLlib & có kết quả TN1, TN2. |
| **Giai đoạn 3:**  26/11 – 30/11 *(5 ngày)* | • Rà soát lại giải thích tính đại diện của Subset (10 cửa hàng) để đưa vào báo cáo. • Chuẩn bị phần trình bày tiếng Anh cho phần 1 (khoảng 4.5 phút). | • Phân tích ưu/nhược điểm của từng mô hình (tại sao GBT lâu nhưng chính xác hơn LR). • Chuẩn bị phần trình bày tiếng Anh cho phần 2 (khoảng 4.5 phút). | • Chạy **Thí nghiệm 3 (Data Scalability)** & **Thí nghiệm 4 (Resource Scaling)**. • Vẽ toàn bộ biểu đồ kết quả (TN1 $\rightarrow$ TN4) và viết hoàn thiện **Báo cáo tiếng Anh**. | **30/11:** Xong toàn bộ thực nghiệm và bản nháp đầy đủ Báo cáo tiếng Anh. |
| **Giai đoạn 4:**  01/12 – 03/12 *(3 ngày)* | • Đọc chéo báo cáo, kiểm tra lỗi chính tả/ngữ pháp tiếng Anh. • Hoàn thiện phần slide của mình. | • Đọc chéo báo cáo, kiểm tra tính nhất quán số liệu giữa bảng và biểu đồ. • Hoàn thiện phần slide của mình. | • Hoàn thiện **Slide tiếng Anh (12–15 slides)**. • Xuất bản PDF cuối cùng cho Báo cáo và Slide. | **03/12:** Đóng băng (Freeze) toàn bộ code, báo cáo và slide. |
| **Giai đoạn 5:**  04/12 – 05/12 *(2 ngày)* | **Cả nhóm (Thành – Tiến – Thiện):**  • **04/12:** Chạy rehearsal (thuyết trình thử bằng tiếng Anh), bấm giờ chuẩn **15 phút** (mỗi người \~5 phút). • **05/12:** Luyện trả lời bộ câu hỏi Q\&A tiếng Anh (về Shuffle, Parquet, Subset, Trade-off mô hình) và nộp bài. |  |  | **05/12:** Sẵn sàng nộp bài & thuyết trình. |

### **3\. Quy tắc phối hợp để không bị "vỡ" tiến độ**

1. **Quản lý code qua GitHub:** Tạo 1 repo chung chia làm 3 thư mục rõ ràng: /docker (Thành), /src/etl (Thành), /src/ml\_experiments (Tiến & Thiện), /docs (Thiện). Không gửi code qua Zalo/Messenger để tránh lệch phiên bản.  
2. **Chuẩn hóa thông số cụm Docker:** Thống nhất dùng chung 1 file docker-compose.yml ngay từ ngày 20/11. Khi Thiện chạy Thí nghiệm 4 (thay đổi số lượng Worker), chỉ cần dùng lệnh:  
3. Bash

```
docker-compose up -d --scale spark-worker=3
```

4.   
   rất nhanh gọn mà không phải sửa lại cấu hình của Thành và Tiến.  
5. **Ghi chép bằng tiếng Anh ngay từ đầu:** Các ghi chú về chỉ số, tên biểu đồ, và nhận xét rút ra sau mỗi thí nghiệm nên được note thẳng bằng tiếng Anh vào file README hoặc Google Docs chung để Thiện ghép vào báo cáo nhanh nhất, tránh mất thời gian dịch thuật vào phút chót.

### **1\. Hạ tầng & Môi trường triển khai (Infrastructure & Containerization)**

* **Docker & Docker Compose:** Công cụ cốt lõi để đóng gói toàn bộ cụm xử lý phân tán thành các container độc lập. Hỗ trợ lệnh \--scale và giới hạn cứng CPU / RAM cho từng node, giúp **Thiện** dễ dàng chạy thí nghiệm mở rộng tài nguyên (Resource Scaling) mà không cần thuê nhiều máy chủ Cloud.  
* **Base Image (bitnami/spark:3.5 hoặc apache/spark:3.5.1-python3):** Image chuẩn chứa sẵn môi trường Java (JVM) và Apache Spark đã được cấu hình sẵn mạng nội bộ, giúp **Thành** dựng cụm trong 5 phút mà không lo lỗi xung đột phiên bản Java/Hadoop/Spark.

### **2\. Lưu trữ & Định dạng Dữ liệu (Storage & Data Formats)**

* **Local Shared Volume (Docker Bind Mount):** Thư mục dùng chung ánh xạ từ máy host vào các container (/opt/bitnami/spark/data), đóng vai trò như một hệ thống lưu trữ phân tán giả lập (thay thế HDFS/S3) để tất cả Worker cùng đọc/ghi dữ liệu.  
* **Apache Parquet (nén Snappy):** Định dạng lưu trữ hướng cột (Columnar Storage). Dùng để so sánh trực tiếp với định dạng **CSV** truyền thống trong Thí nghiệm 1 (giảm 70–80% dung lượng và tăng tốc độ đọc I/O gấp nhiều lần nhờ cơ chế *Predicate Pushdown*).

### **3\. Xử lý Dữ liệu Lớn & Học máy (Big Data Processing & ML)**

* **Apache Spark (PySpark API):** Engine xử lý dữ liệu phân tán trong bộ nhớ (In-Memory Computing). Nhóm sẽ sử dụng 3 module chính của Spark:  
  * **Spark SQL & DataFrame API:** Dùng để join 6 bảng dữ liệu của *Corporación Favorita*, lọc subset và làm sạch dữ liệu khuyết.  
  * **Spark Window Functions:** Dùng để tính các đặc trưng chuỗi thời gian (lag\_7, lag\_14, rolling\_mean\_7) trên 15 triệu dòng.  
  * **Spark MLlib:** Thư viện Machine Learning phân tán để xây dựng Pipeline (StringIndexer, VectorAssembler) và huấn luyện 3 mô hình (LinearRegression, RandomForestRegressor, GBTRegressor).

### **4\. Môi trường Phát triển & Giám sát (Development & Monitoring)**

* **JupyterLab (tích hợp PySpark Kernel):** Chạy dưới dạng 1 container riêng trong mạng Docker, cung cấp giao diện tương tác trực quan để **Thành** và **Tiến** viết code, kiểm tra schema và debug từng bước.  
* **Spark Web UI (Port 8080 & 4040):** Công cụ giám sát tích hợp sẵn của Spark. Đây là nguồn cung cấp số liệu "vàng" cho **Thiện** đưa vào báo cáo: theo dõi biểu đồ DAG (Directed Acyclic Graph), thời gian thực thi từng Stage, dung lượng bộ nhớ tiêu thụ và lượng dữ liệu **Shuffle Read / Shuffle Write** giữa các Worker.  
* **Docker Stats (docker stats):** Lệnh theo dõi mức tiêu thụ %CPU và Memory Usage theo thời gian thực của từng container Worker khi đang train mô hình.

### **5\. Trực quan hóa, Quản lý Mã nguồn & Tài liệu (Visualization, VCS & Docs)**

* **Matplotlib & Seaborn (Python):** Dùng để vẽ các biểu đồ phân tích dữ liệu (EDA) và 4 biểu đồ kết quả thực nghiệm (Execution Time, Speedup Ratio, RMSE Comparison, Storage Size) với độ phân giải cao (300 DPI) để chèn vào báo cáo.  
* **Git & GitHub:** Quản lý mã nguồn tập trung giữa 3 thành viên (Thành – Tiến – Thiện), đồng bộ hóa file docker-compose.yml và các script PySpark.  
* **Overleaf (LaTeX) hoặc Google Docs:** Soạn thảo báo cáo cuối kỳ bằng tiếng Anh (khuyên dùng Overleaf với template chuẩn IEEE để bài báo cáo trông chuyên nghiệp và khoa học nhất).  
* **Canva / Google Slides / PowerPoint:** Thiết kế slide thuyết trình tiếng Anh và vẽ sơ đồ kiến trúc hệ thống (System Architecture Diagram).

### **Bảng tóm tắt phân bổ Tool theo từng thành viên**

| Thành viên | Công cụ sử dụng chính | Mục đích sử dụng |
| :---- | :---- | :---- |
| **Thành** | Docker Compose, Apache Spark (PySpark SQL), Apache Parquet, JupyterLab | Dựng cụm Spark, đọc CSV, join bảng, lọc subset 15M dòng và xuất ra Parquet. |
| **Tiến** | PySpark Window Functions, Spark MLlib, JupyterLab | Tạo đặc trưng Lag/Rolling, xây dựng ML Pipeline và huấn luyện 3 mô hình dự báo. |
| **Thiện** | Spark Web UI, docker stats, Matplotlib/Seaborn, Overleaf (LaTeX), Google Slides | Đo đạc hiệu năng mở rộng (Scalability), chụp DAG/Shuffle metrics, vẽ biểu đồ và viết báo cáo/slide tiếng Anh. |

