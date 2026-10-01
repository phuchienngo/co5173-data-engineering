## BR4 - Cancellation Monitoring

- **Input:** Nhật ký cập nhật trạng thái đơn đặt (trip_id, status_change, minutes_before_pickup).
- **DE Pipeline:** PySpark phân loại và đếm số lượng đơn hủy theo các mốc thời gian sát giờ nhận dịch vụ để gắn cờ rủi ro.
- **Serving:** Thẻ chỉ số Scorecard cảnh báo tỷ lệ hủy sát giờ trên Looker Studio để kích hoạt ưu đãi điều phối lại xe/phòng.