package acclient

import (
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
)

// Organizations typically use audit logs to track the following types of activity:
// Administrative activity
// This includes events like creating or deleting a user account, such as deleting a user from your CRM tool (e.g., Salesforce).
//
// Data access and modification
// This includes events where a user views, creates, or modifies data, such as downloading a file from payroll software (e.g., Workday).
//
// User denials or login failures
// Audit logs such as Okta and VPN logs may capture when a user is unable to login to a system (e.g., due to invalid credentials) or is denied access to resources like a specific URL.
//
//	System-wide changes
//
// Audit logs from sources like AWS Cloudtrail may capture larger events occurring within a network, such as a user creating a new VM instance or creating a new application.
func AuditLog(accid string, cred *cpb.Credential, event *header.Event) {
}
