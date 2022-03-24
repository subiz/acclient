package acclient

import (
	"github.com/subiz/header"
)

func ListDefaultDefs() []*header.AttributeDefinition {
	return []*header.AttributeDefinition{
		&header.AttributeDefinition{
			Name:     "Google Analytics Client ID",
			Label:    "Google Analytics Client ID",
			Key:      "ga_client_id",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:       "First Paid Order",
			Label:      "First Paid Order",
			Key:        "first_paid_order_id",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:     "User has been blocked",
			Label:    "User has been blocked",
			Key:      "is_ban",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "boolean",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:        "Avatar",
			Label:       "Ảnh đại diện",
			Description: "Ảnh cá nhân của người dùng",
			I18NLabel: &header.I18NString{
				Vi_VN: "Ảnh đại diện",
				En_US: "Avatar",
			},
			Key:      "avatar_url",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsImage:  true,
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:  "Email",
			Label: "Email",
			I18NLabel: &header.I18NString{
				Vi_VN: "Email",
				En_US: "Email",
			},
			Key:      "emails",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Rank",
			Label:    "Rank",
			Key:      "rank",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "number",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Zalo",
			Label:    "Zalo",
			Key:      "zalo",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "External ID",
			Label:    "External ID",
			Key:      "external_id",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Facebook",
			Label:    "Facebook",
			Key:      "facebook",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Job title",
			Label:    "Job title",
			Key:      "job_title",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Status",
			Label:    "Status",
			Key:      "status",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:        "Fullname",
			Label:       "Họ tên",
			Description: "Họ tên đầy đủ của người dùng",
			I18NLabel: &header.I18NString{
				Vi_VN: "Họ tên",
				En_US: "Fullname",
			},
			Key:      "fullname",
			Kind:     header.AttributeDefinition_default.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Address",
			Label:    "Address",
			Key:      "address",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "City",
			Label:    "City",
			Key:      "city",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "District",
			Label:    "District",
			Key:      "district",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Phone",
			Label:    "Phone",
			Key:      "phones",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Focused",
			Label:    "Focused",
			Key:      "focused",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "boolean",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:       "Gender",
			Label:      "Gender",
			Key:        "gender",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Created",
			Label:      "Created",
			Key:        "created",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "datetime",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Modified",
			Label:      "Modified",
			Key:        "modified",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "datetime",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Seen",
			Label:      "Seen",
			Key:        "seen",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "datetime",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Total conversations",
			Label:      "Total conversations",
			Key:        "total_conversations",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "number",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:     "Total sessions",
			Label:    "Total sessions",
			Key:      "total_sessions",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "number",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "City name",
			Label:    "City name",
			Key:      "trace_city_name",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Country code",
			Label:    "Country code",
			Key:      "trace_country_code",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:     "Country name",
			Label:    "Country name",
			Key:      "trace_country_name",
			Kind:     header.AttributeDefinition_system.String(),
			Type:     "text",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:       "Has subscribe notify desktop",
			Label:      "Has subscribe notify desktop",
			Key:        "desktop_notify_subscribed",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "boolean",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Last message sent time",
			Label:      "Last message sent time",
			Key:        "last_message_sent",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "datetime",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Channel source",
			Label:      "Channel source",
			Key:        "channel_source",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "First channel",
			Label:      "First channel",
			Key:        "first_channel",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Latest channel",
			Label:      "Latest channel",
			Key:        "latest_channel",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "First session channel source",
			Label:      "First session channel source",
			Key:        "first_session_source",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsReadonly: true,
			IsSystem:   true,
		},
		&header.AttributeDefinition{
			Name:       "First session source referer",
			Label:      "First session source referer",
			Key:        "first_session_referer",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:        "Channel source",
			Label:       "Channel source",
			Key:         "first_session_tracking_link",
			Description: "The link that user click from referer site to go to our site",
			Kind:        header.AttributeDefinition_system.String(),
			Type:        "text",
			IsSystem:    true,
			IsReadonly:  true,
		},
		&header.AttributeDefinition{
			Name:       "Latest channel source",
			Label:      "Latest channel source",
			Key:        "lastest_session_source",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Latest session source referer",
			Label:      "Latest session source referer",
			Key:        "latest_session_referer",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:        "Latest session tracking link",
			Label:       "Latest session tracking link",
			Key:         "latest_session_tracking_link",
			Description: "The link that user click from referer site to go to our site",
			Kind:        header.AttributeDefinition_system.String(),
			Type:        "text",
			IsSystem:    true,
			IsReadonly:  true,
		},
		&header.AttributeDefinition{
			Name:       "First channel touchpoint",
			Label:      "First channel touchpoint",
			Key:        "first_channel_touchpoint",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Name:       "Latest channel touchpoint",
			Label:      "Latest channel touchpoint",
			Key:        "latest_channel_touchpoint",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Label: "Thời gian tạo lead",
			I18NLabel: &header.I18NString{
				Vi_VN: "Thời gian tạo lead",
				En_US: "Convert to lead at",
			},
			Type:     "datetime",
			Key:      "lead_at",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:  "Tổng số đơn",
			Label: "Tổng số đơn",
			I18NLabel: &header.I18NString{
				Vi_VN: "Tổng số đơn",
				En_US: "Number of orders",
			},
			Key:        "num_orders",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "number",
			IsSystem:   true,
			IsReadonly: true,
		},
		&header.AttributeDefinition{
			Label:       "Tổng giá trị đơn",
			Description: "Tổng giá trị các đơn đã xác nhận",
			I18NLabel: &header.I18NString{
				Vi_VN: "Tổng giá trị đơn",
				En_US: "Total order value",
			},
			Type:     "number",
			Key:      "total_order_value",
			IsSystem: true,
		},
		&header.AttributeDefinition{
			Name:       "First Interact",
			Label:      "First Interact",
			Key:        "first_interact",
			Kind:       header.AttributeDefinition_system.String(),
			Type:       "text",
			IsSystem:   true,
			IsReadonly: true,
		},
	}
}
