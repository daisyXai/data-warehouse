:Tư duy để chọn bảng Fact và bảng Dim là :
Bảng Fact thì dữ liệu thường thay đổi liên tục và rất nhanh , Nó dùng để đo lường .

Bảng Dim mang tính mô tả .Nó ko phản ảnh quá trình dữ liệu sinh ra mà mang tính phân loại dữ liệu nhiều hơn

Vậy thì cái bài toán này cần xác định các bảng fact trước :
Xác định dựa vào 2 việc :

1. Data thay đổi liên tục và thể hiện ý nghĩa khi nó thay đổi
2. Các bảng fact này phải trả lời được cho nghiệp vụ của bài toán .
   => Ta có 2 bảng fact là order và inventory . Vì khi bán hành thì dữ liệu được tạo ra nhiều nhất là giao dịch order và khi đó kéo theo số lượng hàng trong kho cũng thay đổi liên tục

Vậy ta xác định các bảng DIm trước :
Cần có về địa điểm , ai mua , và khi nào mua , mua gì
Note : Do có yêu cầu về hierarchy nữa , nên cần chia rõ các bảng Dim nhưng đừng quá chi tiết
DimLocation
gộp 2 bảng văn phòng đại diện và cửa hàng vào ta có :
sđt , thời gian,tên thành phố , địa chỉ VP , bang và thời gian .
=> Ko nên tách ra
DimCity và DimStore
Ta cần DimProduct
mô tả , kích thước ,trọng lượng , giá , thời gian .
DimCustomer
Gồm có customer_type ,city_id
DimTimeline

=>>>
Tôi quyết định xây dựng 2 bảng fact là
Fact_đơn_hàng và Fact_kho_hàng
Các bảng Dim gồm 5 bảng
Dim_thành_phố , Dim_cửa_hàng , Dim_sản_phẩm , Dim_khách_hàng,Dim_thời_gian_biểu

## flow tổng thể là : Scheme ->EER -> IER -> IDB -> DW

### Các bảng coi như là mặc định sẽ phải đổi tên từ bước EER -> IER -> IDB -> DW

1.EER Khách_hàng : mã_khách_hàng , tên KH , Mã Thành phố (Cần phải coi đây là 1 khóa ngoại),ngày đặt hàng đầu tiên
2.Khách hàng du lịch:Mã Khách hàng , hướng dẫn viên du lịch , thời gian tạo bản ghi(Đổi tên từ thời gian => thời gian tạo bản ghi ) .
3.Khách hàng bưu điện :Mã khách hàng , địa chỉ bưu điện , thời gian tạo bản ghi .

4.Văn phòng đại diện :(Mã thành phố ,tên thành phố , địa chỉ VP,Bang , thời gian( thời gian tạo bản ghi) )
5.Cửa hàng : Mã cửa hàng , mã thành phố , số điện thoại , thời gian (thời gian tạo bản ghi)
6.Mặt hàng : Mã mặt hàng , mô tả , kích thước , trọng lượng , giá , thời gian (thời gian tạo bản ghi)
7.Đơn đặt hàng :Mã đơn , ngày đặt hàng , mã khách hàng(Biến nó thành khóa ngoại)
8.Mặt hàng được đặt : Mã đơn , mã đặt hàng , số lượng đặt , giá đặt , thời gian (Thời gian tạo bản ghi ) .

#####

🔷 I. DIM TABLES (5 bảng)

1. DIM_THOI_GIAN

👉 Bắt buộc cho OLAP

date_key (PK) -- dạng int: 20260417
full_date -- date
day
month
quarter
year
day_of_week
is_weekend 2. DIM_THANH_PHO
city_key (PK)
ma_thanh_pho -- business key
ten_thanh_pho
bang
dia_chi_vp -- từ bảng văn phòng đại diện

👉 Lưu luôn địa chỉ VP để phục vụ query (yêu cầu 4)

3. DIM_CUA_HANG
   store_key (PK)
   ma_cua_hang
   so_dien_thoai
   city_key (FK)

👉 Không cần nhét quá nhiều info vì đã join sang thành phố

4. DIM_SAN_PHAM
   product_key (PK)
   ma_mat_hang
   mo_ta
   kich_co
   trong_luong
   gia_niem_yet

👉 Giá ở đây là giá chuẩn (master data)

5. DIM_KHACH_HANG
   customer_key (PK)
   ma_kh
   ten_kh
   city_key (FK)
   ngay_dat_hang_dau_tien

-- loại khách hàng (rất quan trọng cho câu 9)
customer_type : ENUM (buu_dien , du_lich , ca_hai)

👉 Trick hay:

Không tách bảng khách hàng du lịch / bưu điện nữa
Gom lại thành flag → query cực dễ
🔶 II. FACT TABLES

1. FACT_DON_HANG

👉 Grain: 1 dòng = 1 mặt hàng trong 1 đơn hàng

fact_order_id (PK) -- optional

date_key (FK)
customer_key (FK)
product_key (FK)

-- optional nhưng nên có (phục vụ query 3,5,8)
store_key (FK)

ma_don_hang -- degenerate dimension

-- measures
so_luong_dat
gia_dat
tong_tien -- = so_luong_dat \* gia_dat
🔥 Lưu ý quan trọng:
gia_dat ≠ gia_niem_yet
👉 vì giá có thể thay đổi theo đơn hàng 2. FACT_KHO_HANG

👉 Grain: 1 sản phẩm tại 1 cửa hàng tại 1 thời điểm

fact_inventory_id (PK)

date_key (FK)
store_key (FK)
product_key (FK)

-- measure
so_luong_ton

Oke khi đổ dữ liệu thì cần chú ý :
Đối với các bảng DIm thì khi match sẽ update còn khi không có sẽ tạo thêm
ĐỐi vứoi các bảng Fact thì khi match sẽ tạo mới . Còn khi match thì kệ .

### thiếu logic khi load data vào DW thì handle cái khách hàng là both khi đã có user là khách du lịch và thêm khách bưu điện và ngượu lại thì phải
