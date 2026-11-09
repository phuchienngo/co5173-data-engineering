## BR4 - Revenue Monitoring

- **Input:** Dữ liệu đặt phòng Airbnb dạng luồng (booking_id, listing_id, revenue, nights_booked) và bảng thuộc tính phòng (vị trí, loại phòng, giá mỗi đêm).
- **DE Pipeline:** PySpark Structured Streaming ghép từng đặt phòng với thuộc tính phòng, tính doanh thu (lấy từ sự kiện, nếu thiếu thì giá mỗi đêm × số đêm) và phân nhóm doanh thu thấp / trung bình / cao.
- **Serving:** Dashboard Grafana `br4-data-result`: số đặt phòng, tổng và trung bình doanh thu, số phòng hoạt động, phân bố nhóm doanh thu, doanh thu theo loại phòng và vị trí, theo thời gian.
