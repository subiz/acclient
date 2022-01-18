package acclient

import (
	"github.com/subiz/header"
)

func ListDefaultDefs() []*header.AttributeDefinition {
	return []*header.AttributeDefinition{
		&header.AttributeDefinition{
			Name: "Google Analytics Client ID",
			Key:  "ga_client_id",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "First Paid Order",
			Key:  "first_paid_order_id",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Email",
			Key:  "emails",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Rank",
			Key:  "rank",
			Kind: header.AttributeDefinition_default.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "Zalo",
			Key:  "zalo",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "External ID",
			Key:  "external_id",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Facebook",
			Key:  "facebook",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Job title",
			Key:  "job_title",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Status",
			Key:  "status",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Fullname",
			Key:  "fullname",
			Kind: header.AttributeDefinition_default.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Address",
			Key:  "address",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "City",
			Key:  "city",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "District",
			Key:  "district",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Phone",
			Key:  "phones",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Focused",
			Key:  "focused",
			Kind: header.AttributeDefinition_system.String(),
			Type: "boolean",
		},
		&header.AttributeDefinition{
			Name: "Gender",
			Key:  "gender",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Created",
			Key:  "created",
			Kind: header.AttributeDefinition_system.String(),
			Type: "datetime",
		},
		&header.AttributeDefinition{
			Name: "Modified",
			Key:  "modified",
			Kind: header.AttributeDefinition_system.String(),
			Type: "datetime",
		},
		&header.AttributeDefinition{
			Name: "Seen",
			Key:  "seen",
			Kind: header.AttributeDefinition_system.String(),
			Type: "datetime",
		},
		&header.AttributeDefinition{
			Name: "Total conversations",
			Key:  "total_conversations",
			Kind: header.AttributeDefinition_system.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "Number of orders",
			Key:  "num_orders",
			Kind: header.AttributeDefinition_system.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "Total order value",
			Key:  "total_order_value",
			Kind: header.AttributeDefinition_system.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "Total sessions",
			Key:  "total_sessions",
			Kind: header.AttributeDefinition_system.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "City name",
			Key:  "trace_city_name",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Country code",
			Key:  "trace_country_code",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Country name",
			Key:  "trace_country_name",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Number of assigned Conversations",
			Key:  "total_assigned_conversations",
			Kind: header.AttributeDefinition_system.String(),
			Type: "number",
		},
		&header.AttributeDefinition{
			Name: "Has subscribe notify desktop",
			Key:  "desktop_notify_subscribed",
			Kind: header.AttributeDefinition_system.String(),
			Type: "boolean",
		},
		&header.AttributeDefinition{
			Name: "Last message sent time",
			Key:  "last_message_sent",
			Kind: header.AttributeDefinition_system.String(),
			Type: "datetime",
		},
		&header.AttributeDefinition{
			Name: "Channel source",
			Key:  "channel_source",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "First channel",
			Key:  "first_channel",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Latest channel",
			Key:  "latest_channel",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "First session channel source",
			Key:  "first_session_source",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "First session source referer",
			Key:  "first_session_referer",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name:        "Channel source",
			Key:         "first_session_tracking_link",
			Description: "The link that user click from referer site to go to our site",
			Kind:        header.AttributeDefinition_system.String(),
			Type:        "text",
		},
		&header.AttributeDefinition{
			Name: "Latest channel source",
			Key:  "lastest_session_source",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Latest session source referer",
			Key:  "latest_session_referer",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name:        "Latest session tracking link",
			Key:         "latest_session_tracking_link",
			Description: "The link that user click from referer site to go to our site",
			Kind:        header.AttributeDefinition_system.String(),
			Type:        "text",
		},
		&header.AttributeDefinition{
			Name: "First channel touchpoint",
			Key:  "first_channel_touchpoint",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "Latest channel touchpoint",
			Key:  "latest_channel_touchpoint",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
		&header.AttributeDefinition{
			Name: "First Interact",
			Key:  "first_interact",
			Kind: header.AttributeDefinition_system.String(),
			Type: "text",
		},
	}
}
