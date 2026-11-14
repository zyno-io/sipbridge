package portable

import "github.com/emiago/sipgo/sip"

// The pre-existing semantic corpus keeps an independent SIP parser oracle.
// Reducer implementation and structured builders do not call this helper.
func testRequest(m Message) (*sip.Request, error) {
	v, e := sip.ParseMessage(m.WireBytes())
	if e != nil {
		return nil, e
	}
	return v.(*sip.Request), nil
}
func testResponse(m Message) (*sip.Response, error) {
	v, e := sip.ParseMessage(m.WireBytes())
	if e != nil {
		return nil, e
	}
	return v.(*sip.Response), nil
}
