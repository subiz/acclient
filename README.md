# acclient

Thư viện Go dùng chung cho các service nội bộ Subiz.

## File trên CDN

Các helper trong `cdn.go` dùng gRPC tới `cdn-0.cdn:18288`; kết nối được khởi tạo một lần, độc lập với Cassandra. Địa chỉ này được cố định trong code. Upload byte đi trực tiếp lên R2 rồi gọi CDN hoàn tất file.

```go
import (
    acclient "github.com/subiz/acclient/v2"
    "github.com/subiz/header"
)
```

Các helper hiện có:

```go
func UploadFile(accountID string, req *header.UploadRequest, data []byte) (*header.File, error)
func ImportCDNFile(accountID, sourceURL string, req *header.UploadRequest) (*header.File, error)
func GetCDNFile(accountID, fileID string) (*header.File, error)
func ResizeImage(accountID, fileID string, maxWidth, maxHeight uint32, format string) (*header.File, error)
```

Helper tự tạo context: upload/import/resize có timeout 4 phút, PUT R2 có timeout 2 phút, `GetCDNFile` có timeout 30 giây. Helper không có vòng lặp retry. Upload/import clone request trước khi điền mặc định và không sửa input; request không được nil. `Category` rỗng mặc định là `attachment`. `AccountId` và `Ctx.AccountId`, nếu được truyền, phải khớp account của lời gọi.

Service gọi phải kiểm tra quyền người dùng trước khi dùng helper. Nếu không có credential trong context, helper tạo credential nội bộ loại `subiz`, issuer `acclient`, scope `all`.

### Upload byte có sẵn

```go
file, err := acclient.UploadFile(accid, &header.UploadRequest{
    Name:        "photo.png",
    ContentType: "image/png",
}, imageBytes)
if err != nil {
    return err
}

// Lưu file.Id để lấy lại thông tin hoặc tạo ảnh resize khi cần.
fileID := file.Id
_ = fileID
```

Các tùy chọn dùng trực tiếp field của `header.UploadRequest`:

```go
file, err := acclient.UploadFile(accid, &header.UploadRequest{
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

`UploadFile` giới hạn dữ liệu ở 25.000.000 byte, tự đặt `Size` theo dữ liệu và xóa `Sha256` trên bản sao request. `ContentType` rỗng mặc định là `application/octet-stream`. Tên file được chuẩn hóa ASCII, chỉ giữ chữ, số, dấu `-`, `.`, `_`; tên rỗng sau chuẩn hóa thành `untitled`. `ImportUrl` phải rỗng; để import hãy dùng `ImportCDNFile`.

`Gentext` yêu cầu trích xuất văn bản tài liệu trước khi trả kết quả. `MaxWidth` và `MaxHeight` là giới hạn ảnh đầu vào, không phải yêu cầu resize.

Lỗi sau khi tạo upload session có `upload_id` trong `log.AError.Attrs`. Nếu đã xác nhận PUT thành công, service có thể gọi RPC `FinishUpload` với cùng credential để thử hoàn tất lại mà không upload lại.

### Import URL

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

`ImportCDNFile` chuyển nguồn và các tùy chọn import cho RPC `CreateUpload`; việc tải nguồn, hỗ trợ data URI/file ID, giới hạn import và giá trị mặc định do service CDN xử lý. Helper chỉ kiểm tra nguồn không rỗng sau khi trim, không tự tải hoặc giải mã dữ liệu.

### Lấy thông tin file

```go
file, err := acclient.GetCDNFile(accid, fileID)
if err != nil {
    return err
}
// Dùng thông tin và URL do CDN trả về.
downloadURL := file.Url
_ = downloadURL
```

`GetCDNFile` gọi RPC `GetFile`, kiểm tra account và ID của kết quả rồi trả bản sao `header.File`. Helper không tự tạo hoặc làm mới URL ký; các field URL được trả theo response của CDN.

File ID dùng cho `GetCDNFile` và `ResizeImage` phải gồm 64 ký tự hex thường, có thể có tiền tố dạng `<scope>_` (scope bắt đầu bằng chữ thường, dài tối đa 32 ký tự, chỉ gồm chữ thường, số và dấu `-`). Không truyền URL thay cho file ID.

### Import HEIC và resize sang JPEG

```go
original, err := acclient.ImportCDNFile(accid, sourceURL, &header.UploadRequest{
    Category:         "attachment",
    ImportTypePrefix: "image/",
    ImportTimeout:    15000, // milliseconds cho việc tải nguồn
})
if err != nil {
    return err
}
image, err := acclient.ResizeImage(accid, original.Id, 1024, 1024, "jpeg")
if err != nil {
    return err
}
// image.Url là URL public của ảnh JPEG đã xử lý.
// image.Width và image.Height là kích thước thực, mỗi chiều <= 1024.
// File original không bị thay đổi.
_ = image
```

`ResizeImage` trả `(*header.File, error)` mô tả file ảnh dẫn xuất, gồm ID, MIME, kích thước thực và URL public. File nguồn phải thuộc account. `format` chỉ nhận `jpeg` hoặc `webp`. Mỗi chiều tối đa 4096 pixel; 0 là không giới hạn chiều đó, cả hai bằng 0 chỉ chuyển định dạng. Ảnh giữ tỷ lệ, không crop hoặc phóng lớn. Service CDN cần hỗ trợ RPC `CdnMgr.ResizeImage`.

Helper kiểm tra URL không rỗng, MIME đúng định dạng yêu cầu, kích thước dương và không vượt giới hạn đã truyền trước khi trả kết quả.

## Các helper file qua HTTP

`UploadFileUrl(accid, url)` và `UploadTypedFileUrl(accid, url, extension, filetype)` vẫn gọi API HTTP `/4.0/accounts/<accid>/files/url/download`, có cache kết quả trong bộ nhớ và thư mục `./.cache`. Đây là luồng riêng với `ImportCDNFile`.

`SummaryTextFile` gọi API summary qua HTTP. `HTMLContent2PDF` trả byte PDF; `HTML2PDF` nhận kết quả từ dịch vụ chuyển đổi rồi gọi `UploadFile` với category `other`.
