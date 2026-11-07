## BR3 - Customer Sentiment Analysis

- **Input:** Đánh giá của khách (Inside Airbnb Bangkok: review_id, listing_id, comments) dạng luồng, và bảng thông tin listing (khu vực, loại phòng).
- **DE Pipeline:** PySpark Structured Streaming tách từ khóa (tokenize, bỏ stop word), phân loại cảm xúc bằng VADER (positive / neutral / negative, `non_english` cho đánh giá không phải tiếng Anh), ghép với listing để thống kê theo khu vực.
- **Serving:** Dashboard Grafana `br3-data-result`: số đánh giá, tỷ lệ tích cực, phân bố cảm xúc, cảm xúc theo khu vực và loại phòng, từ khóa khiếu nại nhiều nhất.
