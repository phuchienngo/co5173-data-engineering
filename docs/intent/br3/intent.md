## BR3 - Customer Sentiment Analysis

- **Input:** Bảng dữ liệu đánh giá văn bản dịch vụ (review_id, location_id, rating, review_text).
- **DE Pipeline:** PySpark chạy tác vụ bóc tách từ khóa (NLP tokenization), phân loại phân cực cảm xúc (Tích cực / Tiêu cực) và tính tỷ lệ hài lòng theo địa điểm.  
- **Serving:** Biểu đồ tròn tỷ lệ đánh giá tiêu cực và Word Cloud các từ khóa khiếu nại nhiều nhất trên Looker Studio.