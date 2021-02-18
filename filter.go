package servicebus

import (
	"encoding/xml"
	"errors"
	"io"
	"time"
)

type (
	// TrueFilter represents a always true sql expression which will accept all messages
	TrueFilter struct{}

	// FalseFilter represents a always false sql expression which will deny all messages
	FalseFilter struct{}

	// SQLFilter represents a SQL language-based filter expression that is evaluated against a BrokeredMessage. A
	// SQLFilter supports a subset of the SQL-92 standard.
	//
	// see: https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-sql-filter
	SQLFilter struct {
		Expression string
	}

	// CorrelationFilter holds a set of conditions that are matched against one or more of an arriving message's user
	// and system properties. A common use is to match against the CorrelationId property, but the application can also
	// choose to match against ContentType, Label, MessageId, ReplyTo, ReplyToSessionId, SessionId, To, and any
	// user-defined properties. A match exists when an arriving message's value for a property is equal to the value
	// specified in the correlation filter. For string expressions, the comparison is case-sensitive. When specifying
	// multiple match properties, the filter combines them as a logical AND condition, meaning for the filter to match,
	// all conditions must match.
	CorrelationFilter struct {
		CorrelationID    *string                     `xml:"CorrelationId,omitempty"`
		MessageID        *string                     `xml:"MessageId,omitempty"`
		To               *string                     `xml:"To,omitempty"`
		ReplyTo          *string                     `xml:"ReplyTo,omitempty"`
		Label            *string                     `xml:"Label,omitempty"`
		SessionID        *string                     `xml:"SessionId,omitempty"`
		ReplyToSessionID *string                     `xml:"ReplyToSessionId,omitempty"`
		ContentType      *string                     `xml:"ContentType,omitempty"`
		Properties       CorrelationFilterProperties `xml:"Properties,omitempty"`
	}

	// CorrelationFilterProperties contains custom properties used in CorrelationFilter.
	CorrelationFilterProperties map[string]interface{}
)

// ToFilterDescription will transform the TrueFilter into a FilterDescription
func (tf TrueFilter) ToFilterDescription() FilterDescription {
	return FilterDescription{
		Type:          "TrueFilter",
		SQLExpression: ptrString("1=1"),
	}
}

// ToFilterDescription will transform the FalseFilter into a FilterDescription
func (ff FalseFilter) ToFilterDescription() FilterDescription {
	return FilterDescription{
		Type:          "FalseFilter",
		SQLExpression: ptrString("1!=1"),
	}
}

// ToFilterDescription will transform the SqlFilter into a FilterDescription
func (sf SQLFilter) ToFilterDescription() FilterDescription {
	return FilterDescription{
		Type:          "SqlFilter",
		SQLExpression: &sf.Expression,
	}
}

// ToFilterDescription will transform the CorrelationFilter into a FilterDescription
func (cf CorrelationFilter) ToFilterDescription() FilterDescription {
	return FilterDescription{
		Type:              "CorrelationFilter",
		CorrelationFilter: cf,
	}
}

// MarshalXML implements the xml.Marshaller interface for CorrelationFilterProperties.
func (c CorrelationFilterProperties) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if len(c) == 0 {
		return nil
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	keyvalWrapper := xml.StartElement{Name: xml.Name{Local: "KeyValueOfstringanyType"}}
	for k, v := range c {
		if err := e.EncodeToken(keyvalWrapper); err != nil {
			return err
		}
		if err := e.EncodeElement(k, xml.StartElement{Name: xml.Name{Local: "Key"}}); err != nil {
			return err
		}
		if err := e.EncodeElement(v, xml.StartElement{Name: xml.Name{Local: "Value"}}); err != nil {
			return err
		}
		if err := e.EncodeToken(keyvalWrapper.End()); err != nil {
			return err
		}
	}
	return e.EncodeToken(start.End())
}

// UnmarshalXML implements the xml.Unmarshaller interface for CorrelationFilterProperties.
func (c *CorrelationFilterProperties) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var key string
	var val interface{}
	*c = map[string]interface{}{}
	for {
		tk, err := d.Token()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		switch tt := tk.(type) {
		case xml.StartElement:
			if tt.Name.Local == "Key" {
				err = d.DecodeElement(&key, &tt)
			} else if tt.Name.Local == "Value" {
				val, err = unmarshalFilterValue(d, tt)
			}
			if err != nil {
				return err
			}
		}
		if key != "" && val != nil {
			(*c)[key] = val
			key = ""
			val = nil
		}
	}
}

// helper for unmarshalling filter values into specific types
func unmarshalFilterValue(d *xml.Decoder, start xml.StartElement) (interface{}, error) {
	var t string
	for _, attr := range start.Attr {
		if attr.Name.Local == "type" {
			t = attr.Value
		}
	}
	if t == "" {
		return nil, errors.New("missing type attribute for value")
	}
	switch t {
	case "int":
		var i32 int32
		if err := d.DecodeElement(&i32, &start); err != nil {
			return nil, err
		}
		return i32, nil
	case "long":
		var i64 int64
		if err := d.DecodeElement(&i64, &start); err != nil {
			return nil, err
		}
		return i64, nil
	case "boolean":
		var b bool
		if err := d.DecodeElement(&b, &start); err != nil {
			return nil, err
		}
		return b, nil
	case "double":
		var f64 float64
		if err := d.DecodeElement(&f64, &start); err != nil {
			return nil, err
		}
		return f64, nil
	case "dateTime":
		var tt time.Time
		if err := d.DecodeElement(&tt, &start); err != nil {
			return nil, err
		}
		return tt, nil
	default:
		// taken from the C# impl
		var s string
		if err := d.DecodeElement(&s, &start); err != nil {
			return nil, err
		}
		return s, nil
	}
}
