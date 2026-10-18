# acclient

Thư viện Go dùng chung cho các service nội bộ Subiz.

## CDN mới

Các helper dưới đây dùng CDN mới qua gRPC và upload byte trực tiếp lên R2. Những hàm file cũ giữ nguyên hành vi.

Helper dùng các struct có sẵn trong `github.com/subiz/header`, không yêu cầu truyền `context.Context` và không tạo struct options riêng. Acclient tự tạo context nội bộ có timeout. Service gọi phải kiểm tra quyền của người dùng trước khi gọi helper.

```go
func UploadCDNFile(accountID string, req *header.UploadRequest, data []byte) (*header.File, error)
func ImportCDNFile(accountID, sourceURL string, req *header.UploadRequest) (*header.File, error)
func GetCDNFile(accountID, fileID string) (*header.File, error)
func GetCDNFileURL(accountID, fileID string) (*header.File, error)
func GetCDNFileURLs(accountID string, req *header.BatchCreateReadURLsRequest) ([]*header.File, error)

func PrepareAttachments(accountID string, attachments []*header.Attachment) ([]*header.Attachment, error)
func ResolveAttachmentURLs(accountID string, attachments []*header.Attachment) ([]*header.Attachment, error)
```

`CDN_SERVICE` tùy chọn địa chỉ gRPC, mặc định `cdn-0.cdn:18288`. Connection được khởi tạo một lần, độc lập với kết nối Cassandra của acclient. URL public do service CDN sinh ra; acclient không cấu hình hay hardcode domain CDN. Đặt biến môi trường trước khi dùng helper.

Upload/import và chuẩn hóa một nhóm attachment có timeout 4 phút; PUT R2 có timeout 2 phút; lấy metadata/cấp URL cho một nhóm file có timeout 30 giây. Không tự retry. Helper clone request trước khi điền mặc định; không sửa input của caller.

Các ví dụ sau dùng các import:

```go
import (
    acclient "github.com/subiz/acclient/v2"
    "github.com/subiz/header"
)
```

### Upload byte có sẵn

```go
file, err := acclient.UploadCDNFile(accid, &header.UploadRequest{
    Name:      "photo.png",
}, imageBytes)
if err != nil {
    return err
}

// Lưu file.Id để có thể xin URL mới khi cần.
fileID := file.Id
_ = fileID
```

Các tùy chọn dùng trực tiếp field của `header.UploadRequest`:

```go
file, err := acclient.UploadCDNFile(accid, &header.UploadRequest{
    Name:               "report.pdf",
    Category:           "attachment",
    ContentType:        "application/pdf",
    ContentDisposition: "attachment",
    TtlSeconds:         7 * 24 * 60 * 60,
    Gentext:            true,
}, pdfBytes)
if err != nil {
    return err
}
_ = file
```

`Gentext` yêu cầu trích xuất văn bản tài liệu trước khi trả kết quả. `MaxWidth` và `MaxHeight` là giới hạn ảnh đầu vào, không phải yêu cầu resize.

Lỗi sau khi tạo upload session có `upload_id` trong `log.AError.Attrs`. Nếu đã xác nhận PUT thành công, service có thể gọi RPC `FinishUpload` với cùng credential để thử hoàn tất lại mà không upload lại.

### Import URL hoặc data URI

```go
file, err := acclient.ImportCDNFile(accid, "https://example.com/report.pdf", &header.UploadRequest{})
if err != nil {
    return err
}
_ = file
```

Có thể giới hạn dung lượng, MIME và thời gian tải:

```go
file, err := acclient.ImportCDNFile(accid, sourceURL, &header.UploadRequest{
    ImportMaxSize:    5_000_000,
    ImportTypePrefix: "image/",
    ImportTimeout:    30_000, // milliseconds
    MaxWidth:         1920,
    MaxHeight:        1080,
})
if err != nil {
    return err
}
_ = file
```

Tham số `sourceURL` ghi đè `uploadRequest.ImportUrl` trên bản sao request, không thay đổi đầu vào. Truyền `&header.UploadRequest{}` nếu dùng tùy chọn mặc định.

`sourceURL` cũng nhận data URI ảnh dạng `data:image/png;base64,...` hoặc file ID thuộc CDN mới trong cùng account. Data URI tối đa 2 MiB sau giải mã; giới hạn file chung là 25.000.000 byte. `ImportMaxSize = 0` dùng giới hạn chung; `ImportTimeout = 0` dùng mặc định 60.000 ms. `Force > 0` bỏ qua cache tải URL.

### Lấy metadata hoặc URL tải mới

```go
// Chỉ lấy metadata; không dùng GetCDNFile để xin URL tải.
metadata, err := acclient.GetCDNFile(accid, fileID)
if err != nil {
    return err
}
_ = metadata

// Lấy file kèm URL tải và thumbnail nếu có.
file, err := acclient.GetCDNFileURL(accid, fileID)
if err != nil {
    return err
}
downloadURL := file.Url
thumbnailURL := file.ThumbnailUrl
_, _ = downloadURL, thumbnailURL
```

URL đọc có hạn tối đa 30 phút và không vượt TTL file. Lưu file ID làm tham chiếu lâu dài; xin URL mới khi trả dữ liệu hoặc gửi nội dung. Không coi URL ký là đường dẫn vĩnh viễn.

### Cấp URL theo batch, có resize

```go
files, err := acclient.GetCDNFileURLs(accid, &header.BatchCreateReadURLsRequest{
    ExpiresInSeconds: 1800,
    Items: []*header.CreateReadURLItem{
        {RequestId: "original", Fid: fileID},
        {
            RequestId:      "preview",
            Fid:            fileID,
            ImageMaxWidth:  512,
            ImageMaxHeight: 512,
        },
    },
})
if err != nil {
    return err
}

// Kết quả giữ đúng thứ tự Items, kể cả khi một fid xuất hiện nhiều lần.
for i, file := range files {
    if file.ErrorCode != "" || file.Url == "" {
        // Xử lý file không khả dụng ở vị trí i; không dùng URL rỗng.
        continue
    }
    // Gắn file.Url vào vị trí i của response.
    _, _ = i, file.Url
}
```

Helper tự chia thành các RPC tối đa 100 mục. Mỗi `RequestId` được cung cấp phải duy nhất trong request helper; nếu bỏ trống, helper tự điền `acclient-<vị trí>`. Tránh dùng các ID này cho mục khác. Resize chỉ áp dụng cho ảnh, mỗi chiều từ 0 đến 4096 pixel; cả hai bằng 0 là bản gốc. `err` biểu thị lỗi toàn lời gọi; vẫn phải kiểm tra kết quả từng file khi `err == nil`. File gốc có thể đọc được dù hậu xử lý thất bại; caller có thể xử lý riêng trường hợp có `Url` nhưng có lỗi processing.

### Chuẩn hóa attachment trước khi lưu

```go
attachments, err := acclient.PrepareAttachments(accid, []*header.Attachment{
    {
        Type: "file",
        Name: "report.pdf",
        Url:  "https://example.com/report.pdf",
    },
})
if err != nil {
    return err
}

// Gắn kết quả vào message trước khi lưu.
message.Attachments = attachments
```

Helper import/xác minh file, chuẩn hóa metadata và trả bản sao. Sau khi thành công, attachment dùng CDN mới được đánh dấu `v = "cdn"`; `v` rỗng là dữ liệu legacy. Đây là thao tác chủ động khi chuẩn bị nội dung, không gọi để tự chuyển đổi toàn bộ lịch sử lúc đọc.

### Cấp URL attachment trước khi gửi

```go
// Chỉ gọi sau khi đã kiểm tra quyền đọc và lọc internal message.
attachments, err := acclient.ResolveAttachmentURLs(accid, storedMessage.Attachments)
if err != nil {
    return err
}

// responseMessage là bản sao dành cho người nhận, không phải object trong cache.
responseMessage.Attachments = attachments
```

Helper trả bản sao, cấp URL mới cho attachment có `v = "cdn"` và giữ URL legacy. Quote được duyệt đệ quy theo `v` của từng attachment bên trong. Version khác rỗng/`cdn` bị từ chối. Không ghi kết quả đã cấp URL ký ngược vào DB/cache.

Quy ước lưu trữ của `PrepareAttachments`:

- Attachment file lưu `file_id`, metadata trong `file`, và URL CDN chuẩn bỏ toàn bộ query/fragment, ví dụ `https://r2p.subiz.net/file/ac01/<fid>`.
- Ảnh trong card/button, quick reply, ZaloTemplate/ZaloContact, product/order và form dùng cùng URL chuẩn làm tham chiếu lâu dài. Không thay `link_url` hay URL trang sản phẩm.
- Các URL văn bản và field URL R2/GCS/resize cũ trong metadata `File` không được giữ như URL tải lâu dài.
- `v = "cdn"` chỉ được gắn sau khi chuẩn hóa toàn bộ nhóm thành công. Nếu một import lỗi, helper trả lỗi và không sửa input; những file đã import trước đó có thể đã tồn tại trên CDN.

`ResolveAttachmentURLs` gom tham chiếu trùng trước khi cấp URL. Attachment mới được nhận diện bằng `v = "cdn"`; helper kiểm tra đường dẫn và account, lấy file ID rồi yêu cầu CDN cấp URL hiện tại, không phụ thuộc domain đã lưu. Khi prepare attachment cũ, URL nguồn được gửi nguyên vẹn cho CDN xử lý import. Khi CDN báo file không khả dụng, helper xóa URL của vị trí đó; attachment file có thêm lỗi trong `Attachment.File.ErrorCode`. Những vị trí ảnh chỉ có string URL sẽ để rỗng. Lỗi RPC của cả nhóm trả `error`, không trả một nhóm URL được cấp dở dang.
