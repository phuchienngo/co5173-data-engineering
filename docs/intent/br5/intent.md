## BR5 - Tung promotion vào những mùa thấp điểm du lịch cho Vendors/Merchants

- **Input:** Dữ liệu đặt phòng khách sạn (hotel: City / Resort, arrival_date_year/month/week_number, stays_in_weekend_nights, stays_in_week_nights, adr, reserved_room_type, country - quốc gia của khách, is_canceled).
- **DE Pipeline:** PySpark (batch) làm sạch dữ liệu, loại các đơn đã hủy, suy ra mùa (season) từ tháng đến, tổng hợp nhu cầu thực tế (số đêm phòng, ADR) theo khách sạn × tháng/tuần × loại phòng × quốc gia khách, so với mức đỉnh để xác định giai đoạn thấp điểm và xếp hạng các tổ hợp loại phòng × thị trường quốc gia nên được khuyến mãi.
- **Serving:** Dashboard khuyến cáo chương trình khuyến mãi cho đối tác khách sạn: giai đoạn thấp điểm, loại phòng và thị trường quốc gia nên nhắm tới để lấp đầy công suất phòng trống.
