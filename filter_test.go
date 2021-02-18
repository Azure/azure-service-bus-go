package servicebus

import (
	"encoding/xml"
	"testing"
	"time"
)

func TestMarshallCorrelationFilter(t *testing.T) {
	cf := CorrelationFilter{
		Properties: map[string]interface{}{
			"somestringvalue": "foo",
			"thisisabool":     true,
		},
	}
	_, err := xml.Marshal(cf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshallCorrelationFilter(t *testing.T) {
	const data = `<CorrelationFilter>
<Label>label</Label>
<Properties>
  <KeyValueOfstringanyType>
    <Key>thisisastring</Key>
    <Value type="string">hello</Value>
  </KeyValueOfstringanyType>
  <KeyValueOfstringanyType>
    <Key>somefloatvalue</Key>
    <Value type="double">3.14159</Value>
  </KeyValueOfstringanyType>
  <KeyValueOfstringanyType>
    <Key>haveanint32</Key>
    <Value type="int">123</Value>
  </KeyValueOfstringanyType>
  <KeyValueOfstringanyType>
    <Key>itstoolong</Key>
    <Value type="long">123456789101112</Value>
  </KeyValueOfstringanyType>
  <KeyValueOfstringanyType>
    <Key>itsonlytruefalse</Key>
    <Value type="boolean">true</Value>
  </KeyValueOfstringanyType>
  <KeyValueOfstringanyType>
    <Key>tellmethetime</Key>
    <Value type="dateTime">2020-07-01T22:44:05Z</Value>
  </KeyValueOfstringanyType>
</Properties>
<CorrelationId>abc-123</CorrelationId>
</CorrelationFilter>`
	var cf CorrelationFilter
	if err := xml.Unmarshal([]byte(data), &cf); err != nil {
		t.Fatal(err)
	}
	for k, v := range cf.Properties {
		switch k {
		case "thisisastring":
			s, ok := v.(string)
			if !ok {
				t.Fatalf("expected a string, got a %T", v)
			}
			if s != "hello" {
				t.Fatalf("unexpected value %s", s)
			}
		case "somefloatvalue":
			f, ok := v.(float64)
			if !ok {
				t.Fatalf("expected a float64, got a %T", v)
			}
			if f != 3.14159 {
				t.Fatalf("unexpected value %f", f)
			}
		case "haveanint32":
			i32, ok := v.(int32)
			if !ok {
				t.Fatalf("expected a int32, got a %T", v)
			}
			if i32 != 123 {
				t.Fatalf("unexpected value %d", i32)
			}
		case "itstoolong":
			i64, ok := v.(int64)
			if !ok {
				t.Fatalf("expected a int64, got a %T", v)
			}
			if i64 != 123456789101112 {
				t.Fatalf("unexpected value %d", i64)
			}
		case "itsonlytruefalse":
			b, ok := v.(bool)
			if !ok {
				t.Fatalf("expected a bool, got a %T", v)
			}
			if b != true {
				t.Fatalf("unexpected value %v", b)
			}
		case "tellmethetime":
			tt, ok := v.(time.Time)
			if !ok {
				t.Fatalf("expected a time.Time, got a %T", v)
			}
			ex, er := time.Parse(time.RFC3339, "2020-07-01T22:44:05Z")
			if er != nil {
				t.Fatal(er)
			}
			if tt != ex {
				t.Fatalf("unexpected value %v", tt)
			}
		}
	}
}
