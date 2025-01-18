package s3

type Metadata struct {
	PartnerID     int `json:"partnerId"`
	AppID         int `json:"appId"`
	DeviceModelID int `json:"deviceModelId"`
}

type PresignedUrlResponse struct {
	ID           int    `json:"id"`
	PresignedUrl string `json:"presignedUrl"`
}
